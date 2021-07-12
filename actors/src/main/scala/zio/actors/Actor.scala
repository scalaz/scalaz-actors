package zio.actors

import zio.actors.Actor.PendingMessage
import zio.clock.Clock
import zio.stream.ZStream
import zio.{ Supervisor => _, _ }

import scala.language.implicitConversions

object Actor {

  private[actors] type PendingMessage[F[_], A] = (F[A], Promise[Throwable, A])
  trait ActorResponse[R, S, +A] {
    type ResponseType

    def materialize[AA >: A](
      state: Ref[S],
      promise: Promise[Throwable, AA],
      supervisor: Supervisor[R]
    ): URIO[R with Clock, Unit]
  }

  object ActorResponse {
    implicit def oneTime[R, S, A](response: RIO[R, (S, A)]): ActorResponse[R, S, A]                         =
      new ActorResponse[R, S, A] {
        override type ResponseType = RIO[R, (S, A)]

        override def materialize[AA >: A](
          state: Ref[S],
          promise: Promise[Throwable, AA],
          supervisor: Supervisor[R]
        ): URIO[R with Clock, Unit] = {
          val completer = ((s: S, a: A) => (state.set(s) *> promise.succeed(a)).ignore).tupled
          response.foldM(
            e =>
              supervisor
                .supervise[R, (S, A)](response, e)
                .foldM(_ => promise.fail(e).ignore, completer),
            completer
          )
        }
      }
    implicit def stream[R, S, E, I](response: ZStream[R, E, (S, I)]): ActorResponse[R, S, ZStream[R, E, I]] =
      new ActorResponse[R, S, ZStream[R, E, I]] {
        override type ResponseType = ZStream[R, E, (S, I)]

        override def materialize[AA >: ZStream[R, E, I]](
          state: Ref[S],
          promise: Promise[Throwable, AA],
          supervisor: Supervisor[R]
        ): URIO[R with Clock, Unit] = {
          // TODO: Supervision
          val outputStream = response.mapM { case (s, a) => state.set(s) *> UIO(a) }
          promise.succeed(outputStream).ignore
        }
      }
  }

  /**
   * Description of actor behavior (can act as FSM)
   *
   * @tparam R environment type
   * @tparam S state type that is updated every message digested
   * @tparam F message DSL
   */
  trait Stateful[R, S, -F[+_]] extends AbstractStateful[R, S, F] {

    /**
     * Override method triggered on message received
     *
     * @param state available for this method
     * @param msg - message received
     * @param context - provisions actor's self (as ActorRef) and actors' creation and selection
     * @tparam A - domain of return entities
     * @return effectful result
     */
    def receive[A](state: S, msg: F[A], context: Context): RIO[R, (S, A)]          = ???
    def receiveS[A](state: S, msg: F[A], context: Context): ActorResponse[R, S, A] =
      ActorResponse.oneTime(receive(state, msg, context))

    /* INTERNAL API */

    final override def makeActor(
      supervisor: Supervisor[R],
      context: Context,
      optOutActorSystem: () => Task[Unit],
      mailboxSize: Int = DefaultActorMailboxSize
    )(initial: S): URIO[R with Clock, Actor[F]] = {

      def process[A](msg: PendingMessage[F, A], state: Ref[S]): URIO[R with Clock, Unit] =
        for {
          s            <- state.get
          (fa, promise) = msg
          receiver      = receiveS(s, fa, context)
          _            <- receiver.materialize(state, promise, supervisor)
        } yield ()

      for {
        state <- Ref.make(initial)
        queue <- Queue.bounded[PendingMessage[F, _]](mailboxSize)
        _     <- (for {
                     t <- queue.take
                     _ <- process(t, state)
                   } yield ()).forever.fork
      } yield new Actor[F](queue)(optOutActorSystem)
    }
  }

  private[actors] trait AbstractStateful[R, S, -F[+_]] {

    private[actors] def makeActor(
      supervisor: Supervisor[R],
      context: Context,
      optOutActorSystem: () => Task[Unit],
      mailboxSize: Int = DefaultActorMailboxSize
    )(initial: S): RIO[R with Clock, Actor[F]]

  }

  private[actors] val DefaultActorMailboxSize = 10000

}

private[actors] final class Actor[-F[+_]](
  queue: Queue[PendingMessage[F, _]]
)(optOutActorSystem: () => Task[Unit]) {
  def ?[A](fa: F[A]): Task[A] =
    for {
      promise <- Promise.make[Throwable, A]
      _       <- queue.offer((fa, promise))
      value   <- promise.await
    } yield value

  def !(fa: F[_]): Task[Unit] =
    for {
      promise <- Promise.make[Throwable, Any]
      _       <- queue.offer((fa, promise))
    } yield ()

  def unsafeOp(command: Command): Task[Any] =
    command match {
      case Command.Ask(msg)  =>
        this ? msg.asInstanceOf[F[_]]
      case Command.Tell(msg) =>
        this ! msg.asInstanceOf[F[_]]
      case Command.Stop      =>
        this.stop
    }

  val stop: Task[List[_]] =
    for {
      tall <- queue.takeAll
      _    <- queue.shutdown
      _    <- optOutActorSystem()
    } yield tall
}
