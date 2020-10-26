package jms4s.jms

import cats.effect.{ Async, Spawn, Sync }
import cats.syntax.all._
import javax.jms.JMSConsumer

class JmsMessageConsumer[F[_]: Async] private[jms4s] (
  private[jms4s] val wrapped: JMSConsumer
) {

  val receiveJmsMessage: F[JmsMessage] =
    for {
      recOpt <- Sync[F].blocking(Option(wrapped.receiveNoWait()))
      rec <- recOpt match {
              case Some(message) => Sync[F].pure(new JmsMessage(message))
              case None          => Spawn[F].cede >> receiveJmsMessage
            }
    } yield rec
}
