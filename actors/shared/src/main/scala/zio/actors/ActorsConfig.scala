package zio.actors

import zio.config.ConfigDescriptor
import zio.config.ConfigDescriptor._
import zio.config.typesafe.TypesafeConfig
import zio.{ Tag, Task, ZIO }

private[actors] object ActorsConfig {

  final case class Addr(value: String) extends AnyVal
  final case class Port(value: Int)    extends AnyVal
  final case class RemoteConfig(addr: Addr, port: Port)

  val remoteConfig: ConfigDescriptor[Option[RemoteConfig]] =
    nested("remoting") {
      (string("hostname").xmap[Addr](Addr, _.value) |@|
        int("port").xmap[Port](Port, _.value))(RemoteConfig.apply, RemoteConfig.unapply)
    }.optional

  private def selectiveSystemConfig[T](systemName: String, configT: ConfigDescriptor[T]) =
    nested(systemName) {
      nested("zio") {
        nested("actors") {
          configT
        }
      }
    }

  def getConfig[T](
    systemName: String,
    configStr: String,
    configDescriptor: ConfigDescriptor[T]
  )(implicit tag: Tag[T]): Task[T] =
    ZIO
      .service[T]
      .provideLayer(TypesafeConfig.fromHoconString[T](configStr, selectiveSystemConfig(systemName, configDescriptor)))

  def getRemoteConfig(systemName: String, configStr: String): Task[Option[RemoteConfig]] =
    getConfig(systemName, configStr, remoteConfig)

}
