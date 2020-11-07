package zio.actors

private[actors] final class Envelope(val command: Command, val recipient: String) extends Serializable

private[actors] sealed trait Command
private[actors] object Command {
  case class Ask(msg: Any)  extends Command
  case class Tell(msg: Any) extends Command
  case object Stop          extends Command
}
