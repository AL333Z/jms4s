/*
 * Copyright (c) 2020 Functional Programming in Bologna
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import cats.effect.{ ExitCode, IO, IOApp, Resource }
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.JmsClient
import jms4s.config.{ QueueName, TopicName }
import jms4s.jms.MessageFactory

import scala.concurrent.duration._

class AckConsumerExample extends IOApp {

  val contextRes: Resource[IO, JmsClient[IO]] = null // see providers section!
  val inputQueue: QueueName                   = QueueName("YUOR.INPUT.QUEUE")
  val outputTopic: TopicName                  = TopicName("YUOR.OUTPUT.TOPIC")

  def yourBusinessLogic(text: String, mf: MessageFactory[IO]): IO[AckAction[IO]] =
    if (text.toInt % 2 == 0)
      mf.makeTextMessage("a brand new message").map(newMsg => AckAction.send(newMsg, outputTopic))
    else if (text.toInt % 3 == 0)
      IO.pure(AckAction.noAck)
    else
      IO.pure(AckAction.ack)

  override def run(args: List[String]): IO[ExitCode] = {
    val consumerRes = for {
      client   <- contextRes
      consumer <- client.createAcknowledgerConsumer(inputQueue, 10, 100.millis)
    } yield consumer

    consumerRes.use(_.handle { (jmsMessage, mf) =>
      for {
        text <- jmsMessage.asTextF[IO]
        res  <- yourBusinessLogic(text, mf)
      } yield res
    }.as(ExitCode.Success))
  }
}
