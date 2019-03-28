package scalaz.actors

import scalaz.actors.Actor.Stateful
import scalaz.zio.{ DefaultRuntime, IO, Ref, Schedule }
import testz.{ Harness, assert }

final class ActorsSuite extends DefaultRuntime {

  def tests[A](harness: Harness[A]): A = {
    import harness._

    section(
      test("Process messages sequentially") { () =>
        sealed trait Message[+ _]
        case object Reset    extends Message[Unit]
        case object Increase extends Message[Unit]
        case object Get      extends Message[Int]

        val handler = new Stateful[Int, Nothing, Message] {
          override def receive[A](state: Int, msg: Message[A]): IO[Nothing, (Int, A)] =
            msg match {
              case Reset    => IO.succeedLazy((0, ()))
              case Increase => IO.succeedLazy((state + 1, ()))
              case Get      => IO.succeedLazy((state, state))
            }
        }

        val io = for {
          actor <- Actor.stateful(Supervisor.none)(0)(handler)
          _     <- actor ! Increase
          _     <- actor ! Increase
          c1    <- actor ! Get
          _     <- actor ! Reset
          c2    <- actor ! Get
        } yield ((c1, c2))

        val (c1, c2) = unsafeRun(io)
        assert(c1 == 2 && c2 == 0)
      },
      test("Propagate errors to supervisor") { () =>
        sealed trait Message[+ _]
        case object Tick extends Message[Unit]

        val maxRetries = 10

        def makeHandler(ref: Ref[Int]): Actor.Stateful[Unit, String, Message] =
          new Stateful[Unit, String, Message] {
            override def receive[A](state: Unit, msg: Message[A]): IO[String, (Unit, A)] =
              msg match {
                case Tick =>
                  ref
                    .update(_ + 1)
                    .flatMap { v =>
                      if (v < maxRetries) IO.fail("fail") else IO.succeed((state, state))
                    }
              }
          }

        val counter = for {
          ref      <- Ref.make(0)
          handler  = makeHandler(ref)
          schedule = Schedule.recurs(maxRetries)
          policy   = Supervisor.retry(schedule)
          actor    <- Actor.stateful(policy)(())(handler)
          _        <- actor ! Tick
          count    <- ref.get
        } yield count

        assert(unsafeRun(counter) == maxRetries)
      }
    )
  }
}
