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

package jms4s.basespec.providers

import cats.data.NonEmptyList
import cats.effect.{ Async, IO, Resource }
import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.GenericContainer.DockerImage
import jms4s.JmsClient
import jms4s.basespec.Jms4sBaseSpec
import jms4s.ibmmq.ibmMQ
import jms4s.ibmmq.ibmMQ._
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.wait.strategy.Wait

trait IbmMQBaseSpec extends Jms4sBaseSpec {

  lazy val ibmMqContainer: GenericContainer = GenericContainer(
    dockerImage = DockerImage(Left("ibmcom/mq:9.2.2.0-r1")),
    exposedPorts = Seq(1418, 9443),
    env = Map(
      "LICENSE"      -> "accept",
      "MQ_QMGR_NAME" -> "QM1",
      "MQ_DEV"       -> "true"
    ),
    classpathResourceMapping = Seq(("definitions.mqsc", "/etc/mqm/definitions.mqsc", BindMode.READ_ONLY)),
    waitStrategy = Wait.forLogMessage(".*The listener 'SYSTEM.LISTENER.TCP.1' has started.*", 1)
  )

  override def jmsClientRes(implicit A: Async[IO]): Resource[IO, JmsClient[IO]] =
    ibmMQ.makeJmsClient[IO](
      Config(
        qm = QueueManager("QM1"),
        endpoints = NonEmptyList.one(Endpoint(ibmMqContainer.host, ibmMqContainer.mappedPort(1414))),
        channel = Channel("DEV.APP.SVRCONN"),
        username = Some(Username("app")),
        password = None,
        clientId = ClientId("jms-specs")
      )
    )

}
