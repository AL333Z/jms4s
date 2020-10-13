package jms4s.jms

import cats.effect.Sync
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger
import javax.jms.JMSConsumer

class JmsMessageConsumer[F[_]: Sync: Logger] private[jms4s] (
  private[jms4s] val wrapped: JMSConsumer
) {

  val receiveJmsMessage: F[JmsMessage] =
    for {
      recOpt <- Sync[F].blocking(Option(wrapped.receiveNoWait()))
      rec <- recOpt match {
              case Some(message) => Sync[F].pure(new JmsMessage(message))
              case None          => receiveJmsMessage
            }
    } yield rec
}
