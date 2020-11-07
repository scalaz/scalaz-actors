package zio.actors.examples.persistence

import java.time.Instant

import zio.UIO
import zio.actors.persistence.PersistenceId.PersistenceId
import zio.actors.{ persistence, Context }
import zio.actors.persistence._
import zio.clock.Clock

/**
 * This is a full example of [[https://github.com/akka/akka-samples/tree/2.6/akka-sample-persistence-scala Akka persistence shopping cart]]
 * rewritten in ZIO-Actors together with tests.
 */
object ShoppingCart {

  final case class State(items: Map[String, Int], checkoutDate: Option[Instant]) {

    def isCheckedOut: Boolean =
      checkoutDate.isDefined

    def hasItem(itemId: String): Boolean =
      items.contains(itemId)

    def isEmpty: Boolean =
      items.isEmpty

    def updateItem(itemId: String, quantity: Int): State =
      quantity match {
        case 0 => copy(items = items - itemId)
        case _ => copy(items = items + (itemId -> quantity))
      }

    def removeItem(itemId: String): State =
      copy(items = items - itemId)

    def checkout(now: Instant): State =
      copy(checkoutDate = Some(now))

    def toSummary: Summary =
      Summary(items, isCheckedOut)
  }
  object State {
    val empty = State(items = Map.empty, checkoutDate = None)
  }

  sealed trait Command[+_]
  final case class AddItem(itemId: String, quantity: Int)            extends Command[Confirmation]
  final case class RemoveItem(itemId: String)                        extends Command[Confirmation]
  final case class AdjustItemQuantity(itemId: String, quantity: Int) extends Command[Confirmation]
  final case object Checkout                                         extends Command[Confirmation]
  final case object Get                                              extends Command[Summary]

  final case class Summary(items: Map[String, Int], checkedOut: Boolean)

  sealed trait Confirmation
  final case class Accepted(summary: Summary) extends Confirmation
  final case class Rejected(reason: String)   extends Confirmation

  sealed trait Event {
    def cartId: String
  }

  final case class ItemAdded(cartId: String, itemId: String, quantity: Int)               extends Event
  final case class ItemRemoved(cartId: String, itemId: String)                            extends Event
  final case class ItemQuantityAdjusted(cartId: String, itemId: String, newQuantity: Int) extends Event
  final case class CheckedOut(cartId: String, eventTime: Instant)                         extends Event

  def apply(cartId: String): EventSourcedStateful[Clock, State, Command, Event] =
    new EventSourcedStateful[Clock, State, Command, Event](PersistenceId(cartId)) {
      override def receive[A](
        state: State,
        msg: Command[A],
        context: Context
      ): UIO[(persistence.Command[Event], State => A)] =
        if (state.isCheckedOut) checkedOutShoppingCart[A](cartId, state, msg)
        else openShoppingCart[A](cartId, state, msg)

      override def sourceEvent(state: State, event: Event): State =
        event match {
          case ItemAdded(_, itemId, quantity)            => state.updateItem(itemId, quantity)
          case ItemRemoved(_, itemId)                    => state.removeItem(itemId)
          case ItemQuantityAdjusted(_, itemId, quantity) =>
            state.updateItem(itemId, quantity)
          case CheckedOut(_, eventTime)                  => state.checkout(eventTime)
        }
    }

  private def openShoppingCart[A](
    cartId: String,
    state: State,
    command: Command[A]
  ): UIO[(persistence.Command[Event], State => A)] =
    command match {
      case AddItem(itemId, quantity)            =>
        if (state.hasItem(itemId))
          UIO((Command.ignore, _ => Rejected(s"Item '$itemId' was already added to this shopping cart")))
        else if (quantity <= 0)
          UIO((Command.ignore, _ => Rejected("Quantity must be greater than zero")))
        else
          UIO((Command.persist(ItemAdded(cartId, itemId, quantity)), updatedState => Accepted(updatedState.toSummary)))
      case RemoveItem(itemId)                   =>
        if (state.hasItem(itemId))
          UIO((Command.persist(ItemRemoved(cartId, itemId)), updatedState => Accepted(updatedState.toSummary)))
        else
          UIO((Command.ignore, _ => Accepted(state.toSummary)))
      case AdjustItemQuantity(itemId, quantity) =>
        if (quantity <= 0)
          UIO((Command.ignore, _ => Rejected("Quantity must be greater than zero")))
        else if (state.hasItem(itemId))
          UIO(
            (
              Command.persist(ItemQuantityAdjusted(cartId, itemId, quantity)),
              updatedCart => Accepted(updatedCart.toSummary)
            )
          )
        else
          UIO((Command.ignore, _ => Rejected(s"Cannot adjust quantity for item '$itemId'. Item not present on cart")))

      case Checkout =>
        if (state.isEmpty)
          UIO((Command.ignore, _ => Rejected("Cannot checkout an empty shopping cart")))
        else
          UIO((Command.persist(CheckedOut(cartId, Instant.now())), updatedCart => Accepted(updatedCart.toSummary)))
      case Get      =>
        UIO((Command.ignore, _ => state.toSummary))
    }

  private def checkedOutShoppingCart[A](
    cartId: String,
    state: State,
    command: Command[A]
  ): UIO[(persistence.Command[Event], State => A)] =
    command match {
      case Get                   =>
        UIO((Command.ignore, _ => state.toSummary))
      case _: AddItem            =>
        UIO((Command.ignore, _ => Rejected(s"Can't add an item to an already checked out $cartId shopping cart")))
      case _: RemoveItem         =>
        UIO((Command.ignore, _ => Rejected(s"Can't remove an item from an already checked out $cartId shopping cart")))
      case _: AdjustItemQuantity =>
        UIO((Command.ignore, _ => Rejected(s"Can't adjust item on an already checked out $cartId shopping cart")))
      case Checkout              =>
        UIO((Command.ignore, _ => Rejected(s"Can't checkout already checked out $cartId shopping cart")))

    }

}
