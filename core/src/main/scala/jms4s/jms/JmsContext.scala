package jms4s.jms

import cats.effect.{ Async, Resource, Sync }
import cats.syntax.all._
import javax.jms.JMSContext
import jms4s.config.{ DestinationName, QueueName, TopicName }
import jms4s.jms.JmsDestination.{ JmsQueue, JmsTopic }
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.jms.utils.Logger
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

class JmsContext[F[_]: Async](
  private val context: JMSContext
) {

  private val logger = Logger[F]

  def createContext(sessionType: SessionType): Resource[F, JmsContext[F]] =
    Resource
      .make(
        logger.info("Creating context") *> {
          for {
            ctx <- Sync[F].blocking(context.createContext(sessionType.rawAcknowledgeMode))
            _   <- logger.info(s"Context $ctx successfully created")
          } yield ctx
        }
      )(context =>
        logger.info(s"Releasing context $context") *>
          Sync[F].blocking(context.close())
      )
      .map(context => new JmsContext(context))

  def send(destinationName: DestinationName, message: JmsMessage): F[Unit] =
    createDestination(destinationName)
      .flatMap(destination => Sync[F].blocking(context.createProducer().send(destination.wrapped, message.wrapped)))
      .map(_ => ())

  def send(destinationName: DestinationName, message: JmsMessage, delay: FiniteDuration): F[Unit] =
    for {
      destination <- createDestination(destinationName)
      p           <- Sync[F].delay(context.createProducer())
      _ <- Sync[F].delay(p.setDeliveryDelay(delay.toMillis)) *>
            Sync[F].blocking(p.send(destination.wrapped, message.wrapped))
    } yield ()

  def createJmsConsumer(destinationName: DestinationName): Resource[F, JmsMessageConsumer[F]] =
    for {
      destination <- Resource.liftF(createDestination(destinationName))
      consumer <- Resource.make(
                   logger.info(s"Creating consumer for destination $destinationName") *>
                     Sync[F].blocking(context.createConsumer(destination.wrapped))
                 )(consumer =>
                   logger.info(s"Closing consumer for destination $destinationName") *>
                     Sync[F].blocking(consumer.close())
                 )
    } yield new JmsMessageConsumer[F](consumer)

  def createTextMessage(value: String): F[JmsTextMessage] =
    Sync[F].delay(new JmsTextMessage(context.createTextMessage(value)))

  def commit: F[Unit] = Sync[F].blocking(context.commit())

  def rollback: F[Unit] = Sync[F].blocking(context.rollback())

  private def createQueue(queue: QueueName): F[JmsQueue] =
    Sync[F].delay(new JmsQueue(context.createQueue(queue.value)))

  private def createTopic(topicName: TopicName): F[JmsTopic] =
    Sync[F].delay(new JmsTopic(context.createTopic(topicName.value)))

  def createDestination(destination: DestinationName): F[JmsDestination] = destination match {
    case q: QueueName => createQueue(q).widen[JmsDestination]
    case t: TopicName => createTopic(t).widen[JmsDestination]
  }

}
