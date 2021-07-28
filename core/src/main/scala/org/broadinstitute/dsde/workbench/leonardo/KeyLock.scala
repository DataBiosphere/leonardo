package org.broadinstitute.dsde.workbench.leonardo

import cats.syntax.all._
import cats.effect.Async
import cats.effect.std.{Dispatcher, Semaphore}
import com.google.common.cache.{CacheBuilder, CacheLoader}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/**
 * A functional lock which works on a per-key basis.
 */
abstract class KeyLock[F[_]: Async, K] {
  def withKeyLock[A](key: K)(fa: F[A]): F[A]
}
object KeyLock {
  def apply[F[_], K <: AnyRef](expiryTime: FiniteDuration, maxSize: Int, dispatcher: Dispatcher[F])(
    implicit F: Async[F]
  ): F[KeyLock[F, K]] =
    F.delay(new KeyLockImpl(expiryTime, maxSize, dispatcher))

  // A KeyLock implementation backed by a guava cache
  final private class KeyLockImpl[F[_], K <: AnyRef](expiryTime: FiniteDuration,
                                                     maxSize: Int,
                                                     dispatcher: Dispatcher[F])(
    implicit F: Async[F]
  ) extends KeyLock[F, K] {

    private val cache = CacheBuilder
      .newBuilder()
      .expireAfterWrite(expiryTime.toSeconds, TimeUnit.SECONDS)
      .maximumSize(maxSize)
      .recordStats
      .build(
        new CacheLoader[K, Semaphore[F]] {
          def load(key: K): Semaphore[F] =
            dispatcher.unsafeRunSync(Semaphore(1L))
        }
      )

    override def withKeyLock[A](key: K)(fa: F[A]): F[A] =
      for {
        lock <- F.delay(cache.get(key))
        res <- lock.permit.use(_ => fa)
      } yield res
  }
}
