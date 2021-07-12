package zio.actors

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, File, ObjectInputStream, ObjectOutputStream }
import java.nio.ByteBuffer

import zio.{ Chunk, IO, Promise, RIO, Ref, Task, UIO, ZIO }
import zio.actors.Actor.{ AbstractStateful, Stateful }
import zio.actors.ActorSystemUtils._
import zio.actors.ActorsConfig._
import zio.clock.Clock
import zio.nio.core.{ Buffer, InetAddress, SocketAddress }
import zio.nio.core.channels.{ AsynchronousServerSocketChannel, AsynchronousSocketChannel }
import zio.stream.ZStream

import scala.io.Source

/**
 * Object providing constructor for Actor System with optional remoting module.
 */
object ActorSystem {

  /**
   * Constructor for Actor System
   *
   * @param sysName    - Identifier for Actor System
   * @param configFile - Optional configuration for a remoting internal module.
   *                     If not provided the actor system will only handle local actors in terms of actor selection.
   *                     When provided - remote messaging and remote actor selection is possible
   * @return instantiated actor system
   */
  def apply(sysName: String, configFile: Option[File] = None): Task[ActorSystem] =
    for {
      initActorRefMap <- Ref.make(Map.empty[String, Any])
      config          <- retrieveConfig(configFile)
      remoteConfig    <- retrieveRemoteConfig(sysName, config)
      actorSystem     <- IO.effect(new ActorSystem(sysName, config, remoteConfig, initActorRefMap, parentActor = None))
      _               <- IO
                           .effectTotal(remoteConfig)
                           .flatMap(_.fold[Task[Unit]](IO.unit)(c => actorSystem.receiveLoop(c.addr, c.port)))
    } yield actorSystem
}

/**
 * Context for actor used inside Stateful which provides self actor reference and actor creation/selection API
 */
final class Context private[actors] (
  private val path: String,
  private val actorSystem: ActorSystem,
  private val childrenRef: Ref[Set[ActorRef[Any]]]
) {

  /**
   * Accessor for self actor reference
   *
   * @return actor reference in a task
   */
  def self[F[+_]]: Task[ActorRef[F]] = actorSystem.select(path)

  /**
   * Creates actor and registers it to dependent actor system
   *
   * @param actorName name of the actor
   * @param sup       - supervision strategy
   * @param init      - initial state
   * @param stateful  - actor's behavior description
   * @tparam S  - state type
   * @tparam F1 - DSL type
   * @return reference to the created actor in effect that can't fail
   */
  def make[R, S, F1[+_]](
    actorName: String,
    sup: Supervisor[R],
    init: S,
    stateful: Stateful[R, S, F1]
  ): ZIO[R with Clock, Throwable, ActorRef[F1]] =
    for {
      actorRef <- actorSystem.make(actorName, sup, init, stateful)
      children <- childrenRef.get
      _        <- childrenRef.set(children + actorRef.asInstanceOf[ActorRef[Any]])
    } yield actorRef

  /**
   * Looks up for actor on local actor system, and in case of its absence - delegates it to remote internal module.
   * If remote configuration was not provided for ActorSystem (so the remoting is disabled) the search will
   * fail with ActorNotFoundException.
   * Otherwise it will always create remote actor stub internally and return ActorRef as if it was found.   *
   *
   * @param path - absolute path to the actor
   * @tparam F1 - actor's DSL type
   * @return task if actor reference. Selection process might fail with "Actor not found error"
   */
  def select[F1[+_]](path: String): Task[ActorRef[F1]] =
    actorSystem.select(path)

  /* INTERNAL API */

  private[actors] def actorSystemName = actorSystem.actorSystemName

  private[actors] def actorSystemConfig = actorSystem.config

}

/**
 * Type representing running instance of actor system provisioning actor herding,
 * remoting and actor creation and selection.
 */
final class ActorSystem private[actors] (
  private[actors] val actorSystemName: String,
  private[actors] val config: Option[String],
  private val remoteConfig: Option[RemoteConfig],
  private val refActorMap: Ref[Map[String, Any]],
  private val parentActor: Option[String]
) {

  /**
   * Creates actor and registers it to dependent actor system
   *
   * @param actorName name of the actor
   * @param sup       - supervision strategy
   * @param init      - initial state
   * @param stateful  - actor's behavior description
   * @tparam S - state type
   * @tparam F - DSL type
   * @return reference to the created actor in effect that can't fail
   */
  def make[R, S, F[+_]](
    actorName: String,
    sup: Supervisor[R],
    init: S,
    stateful: AbstractStateful[R, S, F]
  ): RIO[R with Clock, ActorRef[F]] =
    for {
      map          <- refActorMap.get
      finalName    <- buildFinalName(parentActor.getOrElse(""), actorName)
      _            <- if (map.contains(finalName)) IO.fail(new Exception(s"Actor $finalName already exists")) else IO.unit
      path          = buildPath(actorSystemName, finalName, remoteConfig)
      derivedSystem = new ActorSystem(actorSystemName, config, remoteConfig, refActorMap, Some(finalName))
      childrenSet  <- Ref.make(Set.empty[ActorRef[Any]])
      actor        <- stateful.makeActor(
                        sup,
                        new Context(path, derivedSystem, childrenSet),
                        () => dropFromActorMap(path, childrenSet)
                      )(init)
      _            <- refActorMap.set(map + (finalName -> actor))
    } yield new ActorRefLocal[F](path, actor)

  /**
   * Looks up for actor on local actor system, and in case of its absence - delegates it to remote internal module.
   * If remote configuration was not provided for ActorSystem (so the remoting is disabled) the search will
   * fail with ActorNotFoundException.
   * Otherwise it will always create remote actor stub internally and return ActorRef as if it was found.   *
   *
   * @param path - absolute path to the actor
   * @tparam F - actor's DSL type
   * @return task if actor reference. Selection process might fail with "Actor not found error"
   */
  def select[F[+_]](path: String): Task[ActorRef[F]] =
    for {
      solvedPath                             <- resolvePath(path)
      (pathActSysName, addr, port, actorName) = solvedPath

      actorMap <- refActorMap.get

      actorRef <- if (pathActSysName == actorSystemName)
                    for {
                      actorRef <- actorMap.get(actorName) match {
                                    case Some(value) =>
                                      for {
                                        actor <- IO.effectTotal(value.asInstanceOf[Actor[F]])
                                      } yield new ActorRefLocal(path, actor)
                                    case None        =>
                                      IO.fail(new Exception(s"No such actor $actorName in local ActorSystem."))
                                  }
                    } yield actorRef
                  else
                    for {
                      address  <- InetAddress
                                    .byName(addr.value)
                                    .flatMap(iAddr => SocketAddress.inetSocketAddress(iAddr, port.value))
                    } yield new ActorRefRemote[F](path, address)
    } yield actorRef

  /**
   * Stops all actors within this ActorSystem.
   *
   * @return all actors' unprocessed messages
   */
  def shutdown: Task[List[_]] =
    for {
      systemActors <- refActorMap.get
      actorsDump   <- ZIO.foreach(systemActors.values.toList)(_.asInstanceOf[Actor[Any]].stop)
    } yield actorsDump.flatten

  /* INTERNAL API */

  private[actors] def dropFromActorMap(path: String, childrenRef: Ref[Set[ActorRef[Any]]]): Task[Unit] =
    for {
      solvedPath          <- resolvePath(path)
      (_, _, _, actorName) = solvedPath
      _                   <- refActorMap.update(_ - actorName)
      children            <- childrenRef.get
      _                   <- ZIO.foreach_(children)(_.stop)
      _                   <- childrenRef.set(Set.empty)
    } yield ()

  private def receiveLoop(address: ActorsConfig.Addr, port: ActorsConfig.Port): Task[Unit] =
    for {
      addr      <- InetAddress.byName(address.value)
      address   <- SocketAddress.inetSocketAddress(addr, port.value)
      p         <- Promise.make[Nothing, Unit]
      channel   <- AsynchronousServerSocketChannel()
      loopEffect = for {
                     _ <- channel.bind(address)

                     _ <- p.succeed(())

                     loop = for {
                              worker          <- channel.accept
                              obj             <- readFromWire(worker)
                              envelope         = obj.asInstanceOf[Envelope]
                              actorMap        <- refActorMap.get
                              remoteActorPath <- resolvePath(envelope.recipient).map(_._4)
                              _               <- actorMap.get(remoteActorPath) match {
                                                   case Some(value) =>
                                                     for {
                                                       actor    <-
                                                         IO
                                                           .effect(value.asInstanceOf[Actor[Any]])
                                                           .mapError(throwable =>
                                                             new Exception(s"System internal exception - ${throwable.getMessage}")
                                                           )
                                                       response <- actor.unsafeOp(envelope.command).either
                                                       _        <- response match {
                                                                     case Right(
                                                                           stream: zio.stream.ZStream[
                                                                             Any @unchecked,
                                                                             Throwable @unchecked,
                                                                             Any @unchecked
                                                                           ]
                                                                         ) =>
                                                                       (stream.map(StreamMsg) ++ ZStream(StreamEnd)).foreach(e =>
                                                                         writeToWire(worker, e)
                                                                       )
                                                                     case _ => writeToWire(worker, response)
                                                                   }
                                                     } yield ()
                                                   case None        =>
                                                     for {
                                                       responseError <- IO.fail(new Exception("No such remote actor")).either
                                                       _             <- writeToWire(worker, responseError)
                                                     } yield ()
                                                 }
                            } yield ()
                     _   <- loop.forever
                   } yield ()
      _         <- loopEffect.onTermination(_ => channel.close.catchAll(_ => ZIO.unit)).fork
      _         <- p.await
    } yield ()
}

/* INTERNAL API */

private[actors] object ActorSystemUtils {

  private val RegexName = "[\\w+|\\d+|(\\-_.*$+:@&=,!~';.)|\\/]+".r

  private val RegexFullPath =
    "^(?:zio:\\/\\/)(\\w+)[@](\\d+\\.\\d+\\.\\d+\\.\\d+)[:](\\d+)[/]([\\w+|\\d+|\\-_.*$+:@&=,!~';.|\\/]+)$".r

  def resolvePath(path: String): Task[(String, Addr, Port, String)] =
    RegexFullPath.findFirstMatchIn(path) match {
      case Some(value) if value.groupCount == 4 =>
        val actorSystemName = value.group(1)
        val address         = Addr(value.group(2))
        val port            = Port(value.group(3).toInt)
        val actorName       = "/" + value.group(4)
        IO.succeed((actorSystemName, address, port, actorName))
      case None                                 =>
        IO.fail(
          new Exception(
            "Invalid path provided. The pattern is zio://YOUR_ACTOR_SYSTEM_NAME@ADDRES:PORT/RELATIVE_ACTOR_PATH"
          )
        )
    }

  private[actors] def buildFinalName(parentActorName: String, actorName: String): Task[String] =
    actorName match {
      case ""            => IO.fail(new Exception("Actor actor must not be empty"))
      case null          => IO.fail(new Exception("Actor actor must not be null"))
      case RegexName(_*) => UIO.effectTotal(parentActorName + "/" + actorName)
      case _             => IO.fail(new Exception(s"Invalid actor name provided $actorName. Valid symbols are -_.*$$+:@&=,!~';"))
    }

  def buildPath(actorSystemName: String, actorPath: String, remoteConfig: Option[RemoteConfig]): String =
    s"zio://$actorSystemName@${remoteConfig.map(c => c.addr.value + ":" + c.port.value).getOrElse("0.0.0.0:0000")}$actorPath"

  def retrieveConfig(configFile: Option[File]): Task[Option[String]] =
    configFile.fold[Task[Option[String]]](Task.none) { file =>
      IO(Source.fromFile(file)).toManaged(f => UIO(f.close())).use(s => IO.some(s.mkString))
    }

  def retrieveRemoteConfig(sysName: String, configStr: Option[String]): Task[Option[RemoteConfig]] =
    configStr.fold[Task[Option[RemoteConfig]]](Task.none)(file => ActorsConfig.getRemoteConfig(sysName, file))

  def objFromByteArray(bytes: Array[Byte]): Task[Any] =
    Task(new ObjectInputStream(new ByteArrayInputStream(bytes))).toManaged(s => UIO(s.close())).use { s =>
      Task(s.readObject())
    }

  def readFromWire(socket: AsynchronousSocketChannel): Task[Any] =
    for {
      size      <- socket.read(4)
      buffer    <- Buffer.byte(size)
      intBuffer <- buffer.asIntBuffer
      toRead    <- intBuffer.get(0)
      content   <- socket.read(toRead)
      bytes      = content.toArray
      obj       <- objFromByteArray(bytes)
    } yield obj

  def objToByteArray(obj: Any): Task[Array[Byte]] =
    for {
      stream <- UIO(new ByteArrayOutputStream())
      bytes  <- Task(new ObjectOutputStream(stream)).toManaged(s => UIO(s.close())).use { s =>
                  Task(s.writeObject(obj)) *> UIO(stream.toByteArray)
                }
    } yield bytes

  def writeToWire(socket: AsynchronousSocketChannel, obj: Any): Task[Unit] =
    for {
      bytes <- objToByteArray(obj)
      _     <- socket.write(Chunk.fromArray(ByteBuffer.allocate(4).putInt(bytes.size).array()))
      _     <- socket.write(Chunk.fromArray(bytes))
    } yield ()
}

sealed trait StreamProtocol
case class StreamMsg(obj: Any) extends StreamProtocol
case object StreamEnd          extends StreamProtocol
