package zio.actors.akka

import akka.actor.typed
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import zio.actors.Actor.Stateful
import zio.actors._
import zio.actors.akka.AkkaBehaviorsUtils._
import zio.test.Assertion._
import zio.test._
import zio.{ IO, Runtime, ZIO }

import scala.concurrent.duration._

object AkkaBehaviorsUtils {

  sealed trait TypedMessage[+_]

  case object HelloFromZio extends TypedMessage[Unit]

  case class PingFromZio(zioSenderActor: ActorRef[ZioMessage]) extends TypedMessage[Unit]

  case class PingToZio(zioReplyToActor: ActorRef[ZioMessage], msg: String) extends TypedMessage[Unit]

  sealed trait ZioMessage[+_]

  case class Ping(akkaActor: AkkaTypedActorRefLocal[TypedMessage]) extends ZioMessage[Unit]

  case class UpdateFromAkka(msg: String) extends ZioMessage[Unit]

  case class PongFromAkka(msg: String) extends ZioMessage[Unit]

  object TestBehavior {
    lazy val runtime = Runtime.default

    def apply(): Behavior[TypedMessage[_]] =
      Behaviors.receiveMessage { message =>
        message match {
          case HelloFromZio                         => ()
          case PingFromZio(zioSenderActor)          => runtime.unsafeRun(zioSenderActor ! PongFromAkka("Pong from Akka"))
          case PingToZio(zioReplyToActor, msgToZio) => runtime.unsafeRun(zioReplyToActor ! PongFromAkka(msgToZio))
        }
        Behaviors.same
      }
  }

}

object AskUtils {
  sealed trait AskMessage[+_]
  case class PingAsk(value: Int, replyTo: typed.ActorRef[Int]) extends AskMessage[Int]

  sealed trait ZioMessage[+_]
  case class GetState(akkaActor: AkkaTypedActorRefLocal[AskMessage]) extends ZioMessage[Int]

  def PingAskDeferred(value: Int): typed.ActorRef[Int] => PingAsk =
    (hiddenRef: typed.ActorRef[Int]) => PingAsk(value, hiddenRef)

  object AskTestBehavior {

    def apply(): Behavior[AskMessage[_]] =
      Behaviors.receiveMessage { message =>
        message match {
          case PingAsk(value, replyTo) => replyTo ! value
        }
        Behaviors.same
      }
  }
}

object ActorsAkkaSpec extends DefaultRunnableSpec {
  def spec =
    suite("Test the basic integration with akka typed actor behavior")(
      testM("Send message from zioActor to akkaActor") {
        import AkkaBehaviorsUtils._

        val handler = new Stateful[Any, Int, ZioMessage] {
          override def receive[A](
            state: Int,
            msg: ZioMessage[A],
            context: Context
          ): ZIO[Any, Throwable, (Int, A)] =
            msg match {
              case Ping(akkaActor) => (akkaActor ! HelloFromZio).as((state, ()))
              case _               => IO.fail(new Exception("fail"))
            }
        }

        val program = for {
          typedActorSystem <- IO(typed.ActorSystem(TestBehavior(), "typedSystem"))
          system           <- ActorSystem("test1")
          zioActor         <- system.make("actor1", Supervisor.none, 0, handler)
          akkaActor        <- AkkaTypedActor.make(typedActorSystem)
          _                <- zioActor ! Ping(akkaActor)
        } yield ()
        assertM(program.run)(succeeds(anything))
      },
      testM("Send message from akkaActor to zioActor") {
        import AkkaBehaviorsUtils._
        val handler = new Stateful[Any, String, ZioMessage] {
          override def receive[A](
            state: String,
            msg: ZioMessage[A],
            context: Context
          ): IO[Throwable, (String, A)] =
            msg match {
              case PongFromAkka(msg) => IO.succeed((msg, ()))
              case _                 => IO.fail(new Exception("fail"))
            }
        }
        val program = for {
          typedActorSystem <- IO(typed.ActorSystem(TestBehavior(), "typedSystem2"))
          system           <- ActorSystem("test2")
          akkaActor        <- AkkaTypedActor.make(typedActorSystem)
          zioActor         <- system.make("actor2", Supervisor.none, "", handler)
          _                <- akkaActor ! PingToZio(zioActor, "Ping from Akka")
        } yield ()
        assertM(program.run)(succeeds(anything))
      },
      testM("ZioActor send message to akkaActor and then replyTo to zioActor") {
        val handler =
          new Stateful[Any, String, ZioMessage] {
            override def receive[A](
              state: String,
              msg: ZioMessage[A],
              context: Context
            ): IO[Throwable, (String, A)] =
              msg match {
                case Ping(akkaActor)   =>
                  for {
                    self <- context.self[ZioMessage]
                    _    <- akkaActor ! PingFromZio(self)
                  } yield (state, ())
                case PongFromAkka(msg) => IO.succeed((msg, ()))
                case _                 => IO.fail(new Exception("fail"))
              }
          }
        val program = for {
          typedActorSystem <- IO(typed.ActorSystem(TestBehavior(), "typedSystem3"))
          system           <- ActorSystem("test3")
          zioActor         <- system.make("actor3", Supervisor.none, "", handler)
          akkaActor        <- AkkaTypedActor.make(typedActorSystem)
          _                <- zioActor ! Ping(akkaActor)
        } yield ()
        assertM(program.run)(succeeds(anything))
      },
      testM("send ask message to akkaActor and get response") {

        import AskUtils._

        val typedActorSystem = typed.ActorSystem(AskTestBehavior(), "typedSystem")

        implicit val timeout: Timeout           = 3.seconds
        implicit val scheduler: typed.Scheduler = typedActorSystem.scheduler

        def PingAskDeferred(value: Int): typed.ActorRef[Int] => PingAsk =
          (hiddenRef: typed.ActorRef[Int]) => PingAsk(value, hiddenRef)

        for {
          akkaActor <- AkkaTypedActor.make(typedActorSystem)
          result    <- akkaActor ? PingAskDeferred(1000)
        } yield assert(result)(equalTo(1000))
      },
      testM("send message to zioActor and ask akkaActor for the response") {

        import AskUtils._

        val typedActorSystem = typed.ActorSystem(AskTestBehavior(), "typedSystem")

        implicit val timeout: Timeout           = 3.seconds
        implicit val scheduler: typed.Scheduler = typedActorSystem.scheduler

        val handler =
          new Stateful[Any, Int, ZioMessage] {
            override def receive[A](
              state: Int,
              msg: ZioMessage[A],
              context: Context
            ): IO[Throwable, (Int, A)] =
              msg match {
                case GetState(akkaActor) =>
                  (akkaActor ? PingAskDeferred(1000)).map(newState => (newState, newState))
                case _                   => IO.fail(new Exception("fail"))
              }
          }
        for {
          system    <- ActorSystem("test3")
          zioActor  <- system.make("actor3", Supervisor.none, 0, handler)
          akkaActor <- AkkaTypedActor.make(typedActorSystem)
          result    <- zioActor ? GetState(akkaActor)
        } yield assert(result)(equalTo(1000))
      }
    )
}
