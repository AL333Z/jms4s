package jms4s

import cats.effect._
import cats.effect.unsafe.IORuntime
import org.scalatest.{ Assertion, Succeeded }

import scala.concurrent.Future

trait IOSpec {
  implicit val runtime: IORuntime = unsafe.IORuntime.global

  implicit def ioToFutureAssertion(io: IO[Assertion]): Future[Assertion] =
    io.unsafeToFuture()

  implicit def ioUnitToFutureAssertion(io: IO[Unit]): Future[Assertion] =
    io.as(Succeeded).unsafeToFuture()
}
