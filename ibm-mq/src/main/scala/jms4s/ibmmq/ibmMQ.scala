package jms4s.ibmmq

import cats.data.NonEmptyList
import cats.effect.{ Async, Resource, Sync }
import cats.syntax.all._
import com.ibm.mq.jms.MQConnectionFactory
import com.ibm.msg.client.wmq.common.CommonConstants

import jms4s.JmsClient
import jms4s.jms.JmsContext
import jms4s.jms.utils.Logger

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

  def makeJmsClient[F[_]: Async](
    config: Config
  ): Resource[F, JmsClient[F]] = {
    val logger = Logger[F]
    for {
      context <- Resource.make(
                  logger.info(s"Opening Context to MQ at ${hosts(config.endpoints)}...") >>
                    Sync[F].blocking {
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
                  logger.info(s"Closing Context $c at ${hosts(config.endpoints)}...") *>
                    Sync[F].blocking(c.close()) *>
                    logger.info(s"Closed Context $c.")
                )
      _ <- Resource.liftF(logger.info(s"Opened Context $context at ${hosts(config.endpoints)}."))
    } yield new JmsClient[F](new JmsContext[F](context))
  }

  private def hosts(endpoints: NonEmptyList[Endpoint]): String =
    endpoints.map(e => s"${e.host}(${e.port})").toList.mkString(",")

}
