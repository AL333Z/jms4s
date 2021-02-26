package jms4s.ibmmq

import cats.data.NonEmptyList
import cats.effect.{ Blocker, Concurrent, ContextShift, Resource }
import cats.syntax.all._
import com.ibm.mq.jms.MQConnectionFactory
import com.ibm.msg.client.wmq.common.CommonConstants
import org.typelevel.log4cats.Logger
import jms4s.JmsClient
import jms4s.jms.JmsContext

object ibmMQ {

  case class Config(
    qm: QueueManager,
    endpoints: NonEmptyList[Endpoint],
    channel: Channel,
    username: Option[Username] = None,
    password: Option[Password] = None,
    clientId: ClientId
  )
  case class Username(value: String) extends AnyVal
  case class Password(value: String) extends AnyVal
  case class Endpoint(host: String, port: Int)
  case class QueueManager(value: String) extends AnyVal
  case class Channel(value: String)      extends AnyVal
  case class ClientId(value: String)     extends AnyVal

  def makeJmsClient[F[_]: Logger: ContextShift: Concurrent](
    config: Config,
    blocker: Blocker
  ): Resource[F, JmsClient[F]] =
    for {
      context <- Resource.make(
                  Logger[F].info(s"Opening Context to MQ at ${hosts(config.endpoints)}...") >>
                    blocker.delay {
                      val connectionFactory: MQConnectionFactory = new MQConnectionFactory()
                      connectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
                      connectionFactory.setQueueManager(config.qm.value)
                      connectionFactory.setConnectionNameList(hosts(config.endpoints))
                      connectionFactory.setChannel(config.channel.value)
                      connectionFactory.setClientID(config.clientId.value)

                      config.username.map { username =>
                        connectionFactory.createContext(
                          username.value,
                          config.password.map(_.value).getOrElse("")
                        )
                      }.getOrElse(connectionFactory.createContext())
                    }
                )(c =>
                  Logger[F].info(s"Closing Context $c at ${hosts(config.endpoints)}...") *>
                    blocker.delay(c.close()) *>
                    Logger[F].info(s"Closed Context $c.")
                )
      _ <- Resource.liftF(Logger[F].info(s"Opened Context $context at ${hosts(config.endpoints)}."))
    } yield new JmsClient[F](new JmsContext[F](context, blocker))

  private def hosts(endpoints: NonEmptyList[Endpoint]): String =
    endpoints.map(e => s"${e.host}(${e.port})").toList.mkString(",")

}
