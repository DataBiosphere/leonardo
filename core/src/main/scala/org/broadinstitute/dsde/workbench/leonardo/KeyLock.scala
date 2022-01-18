package org.broadinstitute.dsde.workbench.leonardo

import cats.syntax.all._
import cats.effect.Async
import cats.effect.std.Semaphore
import scalacache.caffeine.CaffeineCache

/**
 * A functional lock which works on a per-key basis.
 */
abstract class KeyLock[F[_]: Async, K] {
  def withKeyLock[A](key: K)(fa: F[A]): F[A]
}
object KeyLock {
  def apply[F[_], K <: AnyRef](cache: CaffeineCache[F, K, Semaphore[F]])(
    implicit F: Async[F]
  ): KeyLock[F, K] =
    new KeyLockImpl(cache)

  // A KeyLock implementation backed by a guava cache
  final private class KeyLockImpl[F[_], K <: AnyRef](cache: CaffeineCache[F, K, Semaphore[F]])(
    implicit F: Async[F]
  ) extends KeyLock[F, K] {

    override def withKeyLock[A](key: K)(fa: F[A]): F[A] =
      for {
        lock <- cache.cachingF(key)(None) {
          Semaphore(1L)
        }
        res <- lock.permit.use(_ => fa)
      } yield res
  }
}
