package zio.actors

import java.io.{ IOException, ObjectInputStream, ObjectOutputStream }

import zio.{ Runtime, Task, UIO }

/**
 * Reference to actor that might reside on local JVM instance or be available via remote communication
 *
 * @tparam F wrapper type constructing DSL
 */
sealed trait ActorRef[-F[+_]] extends Serializable {

  /**
   * Send a message to an actor as `ask` interaction pattern -
   * caller is blocked until the response is received
   *
   * @param fa message
   * @tparam A return type
   * @return effectful response
   */
  def ?[A](fa: F[A]): Task[A]

  /**
   * Send message to an actor as `fire-and-forget` -
   * caller is blocked until message is enqueued in stub's mailbox
   *
   * @param fa message
   * @return lifted unit
   */
  def !(fa: F[_]): Task[Unit]

  /**
   * Get referential absolute actor path
   * @return
   */
  val path: UIO[String]

  /**
   * Stops actor and all its children
   */
  val stop: Task[List[_]]

}

/* INTERNAL API */

private[actors] object ActorRefSerial {
  private[actors] val runtimeForResolve = Runtime.default
}

private[actors] sealed abstract class ActorRefSerial[-F[+_]](private var actorPath: String)
    extends ActorRef[F]
    with Serializable {

  @throws[IOException]
  protected def writeObject1(out: ObjectOutputStream): Unit =
    out.writeObject(actorPath)

  @throws[IOException]
  protected def readObject1(in: ObjectInputStream): Unit = {
    val rawActorPath = in.readObject()
    actorPath = rawActorPath.asInstanceOf[String]
  }

  override val path: UIO[String] = UIO(actorPath)
}

private[actors] final class ActorRefLocal[-F[+_]](
  private val actorName: String,
  actor: Actor[F]
) extends ActorRefSerial[F](actorName) {
  override def ?[A](fa: F[A]): Task[A] = actor ? fa

  override def !(fa: F[_]): Task[Unit] = actor ! fa

  override val stop: Task[List[_]] = actor.stop

  @throws[IOException]
  private def writeObject(out: ObjectOutputStream): Unit =
    super.writeObject1(out)

  @throws[IOException]
  private def readObject(in: ObjectInputStream): Unit =
    super.readObject1(in)
}
