/*
 * Copyright 2021 Alessandro Zoffoli
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jms4s

import cats.Functor
import cats.data.NonEmptyList
import cats.effect.{ Concurrent, Resource, Sync }
import cats.syntax.all._
import fs2.Stream
import fs2.concurrent.Queue
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.config.DestinationName
import jms4s.jms._
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

trait JmsAcknowledgerConsumer[F[_]] {
  def handle(f: (JmsMessage, MessageFactory[F]) => F[AckAction[F]]): F[Unit]
}

object JmsAcknowledgerConsumer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    context: JmsContext[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAcknowledgerConsumer[F]] =
    for {
      pool <- Resource.eval(
               Queue.bounded[F, (JmsContext[F], JmsMessageConsumer[F], MessageFactory[F])](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              ctx      <- context.createContext(SessionType.ClientAcknowledge)
              consumer <- ctx.createJmsConsumer(inputDestinationName)
              _        <- Resource.eval(pool.enqueue1((ctx, consumer, MessageFactory[F](ctx))))
            } yield ()
          }
    } yield build(pool, concurrencyLevel, context.blocker)

  private def build[F[_]: ContextShift: Concurrent](
    pool: Queue[F, (JmsContext[F], JmsMessageConsumer[F], MessageFactory[F])],
    concurrencyLevel: Int): JmsAcknowledgerConsumer[F] =
    (f: (JmsMessage, MessageFactory[F]) => F[AckAction[F]]) =>
      Stream
        .emits(0 until concurrencyLevel)
        .as(
          Stream.eval(
            for {
              (context, consumer, mFactory) <- pool.dequeue1
              message                       <- consumer.receiveJmsMessage
              res                           <- f(message, mFactory)
              _ <- res.fold(
                    ifAck = Sync[F].blocking(message.wrapped.acknowledge()),
                    ifNoAck = Sync[F].unit,
                    ifSend = send =>
                      send.messages.messagesAndDestinations.traverse_ {
                        case (message, (name, delay)) =>
                          delay.fold(ifEmpty = context.send(name, message))(
                            f = d => context.send(name, message, d)
                          )
                      } *> Sync[F].blocking(message.wrapped.acknowledge())
                  )
              _ <- pool.enqueue1((context, consumer, mFactory))
            } yield ()
          )
        )
        .parJoin(concurrencyLevel)
        .repeat
        .compile
        .drain

  sealed abstract class AckAction[F[_]] extends Product with Serializable {
    def fold(ifAck: => F[Unit], ifNoAck: => F[Unit], ifSend: AckAction.Send[F] => F[Unit]): F[Unit]
  }

  object AckAction {

    private[jms4s] case class Ack[F[_]]() extends AckAction[F] {
      override def fold(ifAck: => F[Unit], ifNoAck: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] = ifAck
    }

    // if the client wants to ack groups of messages, it'll pass a sequence of NoAck and then a cumulative Ack
    private[jms4s] case class NoAck[F[_]]() extends AckAction[F] {
      override def fold(ifAck: => F[Unit], ifNoAck: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] = ifNoAck
    }

    case class Send[F[_]](messages: ToSend[F]) extends AckAction[F] {

      override def fold(ifAck: => F[Unit], ifNoAck: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] =
        ifSend(this)
    }

    private[jms4s] case class ToSend[F[_]](
      messagesAndDestinations: NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
    )

    def ack[F[_]]: AckAction[F] = Ack()

    def noAck[F[_]]: AckAction[F] = NoAck()

    def sendN[F[_]: Functor](
      messages: NonEmptyList[(JmsMessage, DestinationName)]
    ): Send[F] =
      Send[F](ToSend[F](messages.map { case (message, name) => (message, (name, None)) }))

    def sendNWithDelay[F[_]: Functor](
      messages: NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
    ): Send[F] = Send[F](ToSend(messages))

    def sendWithDelay[F[_]](
      message: JmsMessage,
      destination: DestinationName,
      duration: Option[FiniteDuration]
    ): Send[F] =
      Send[F](ToSend[F](NonEmptyList.one((message, (destination, duration)))))

    def send[F[_]: Functor](message: JmsMessage, destination: DestinationName): Send[F] =
      Send[F](ToSend[F](NonEmptyList.one((message, (destination, None)))))
  }
}
