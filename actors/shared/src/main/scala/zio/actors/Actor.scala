package zio.actors

import zio.actors.Actor.PendingMessage
import zio.clock.Clock
import zio.{ Supervisor => _, _ }

object Actor {

  private[actors] type PendingMessage[F[_], A] = (F[A], Promise[Throwable, A])

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
    def receive[A](state: S, msg: F[A], context: Context): RIO[R, (S, A)]

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
          receiver      = receive(s, fa, context)
          completer     = ((s: S, a: A) => state.set(s) *> promise.succeed(a)).tupled
          _            <- receiver.foldM(
                            e =>
                              supervisor
                                .supervise(receiver, e)
                                .foldM(_ => promise.fail(e), completer),
                            completer
                          )
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
