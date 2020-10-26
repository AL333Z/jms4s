package jms4s.jms.utils

import cats.effect.Sync
import org.log4s.getLogger

object Logger {
  private val logger = getLogger

  def apply[F[_]: Sync]: Logger[F] = new Logger[F] {
    override def info(s: String): F[Unit] = Sync[F].delay(logger.info(s))
  }

}

trait Logger[F[_]] {
  def info(s: String): F[Unit]
}
