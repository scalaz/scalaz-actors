package zio.actors

import zio._
import zio.clock.Clock

private[actors] trait Supervisor[-R] {
  def supervise[R0 <: R, A](zio: RIO[R0, A], error: Throwable): ZIO[R0 with Clock, Unit, A]
}

/**
 *  Supervising policies
 */
object Supervisor {

  final def none: Supervisor[Any] = retry(Schedule.once)

  final def retry[R, A](policy: Schedule[R, Throwable, A]): Supervisor[R] =
    retryOrElse(policy, (_: Throwable, _: A) => IO.unit)

  final def retryOrElse[R, A](
    policy: Schedule[R, Throwable, A],
    orElse: (Throwable, A) => URIO[R, Unit]
  ): Supervisor[R] =
    new Supervisor[R] {
      override def supervise[R0 <: R, A0](zio: RIO[R0, A0], error: Throwable): ZIO[R0 with Clock, Unit, A0] =
        zio
          .retryOrElse(policy, (e: Throwable, a: A) => orElse(e, a) *> ZIO.fail(error))
          .orElseFail(())
    }
}
