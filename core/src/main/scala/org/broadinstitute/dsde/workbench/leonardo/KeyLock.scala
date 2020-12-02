package org.broadinstitute.dsde.workbench.leonardo

import java.util.concurrent.TimeUnit

import cats.effect.concurrent.Semaphore
import cats.effect.implicits._
import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import cats.syntax.all._
import com.google.common.cache.{CacheBuilder, CacheLoader}

import scala.concurrent.duration.FiniteDuration

/**
 * A functional lock which works on a per-key basis.
 */
abstract class KeyLock[F[_], K] {
  def withKeyLock[A](key: K)(fa: F[A]): F[A]
}
object KeyLock {
  def apply[F[_]: ContextShift, K <: AnyRef](expiryTime: FiniteDuration, maxSize: Int, blocker: Blocker)(
    implicit F: ConcurrentEffect[F]
  ): F[KeyLock[F, K]] =
    F.delay(new KeyLockImpl(expiryTime, maxSize, blocker))

  // A KeyLock implementation backed by a guava cache
  final private class KeyLockImpl[F[_]: ContextShift, K <: AnyRef](expiryTime: FiniteDuration,
                                                                   maxSize: Int,
                                                                   blocker: Blocker)(
    implicit F: ConcurrentEffect[F]
  ) extends KeyLock[F, K] {

    private val cache = CacheBuilder
      .newBuilder()
      .expireAfterWrite(expiryTime.toSeconds, TimeUnit.SECONDS)
      .maximumSize(maxSize)
      .recordStats
      .build(
        new CacheLoader[K, Semaphore[F]] {
          def load(key: K): Semaphore[F] =
            Semaphore(1L).toIO.unsafeRunSync()
        }
      )

    override def withKeyLock[A](key: K)(fa: F[A]): F[A] =
      for {
        lock <- blocker.blockOn(F.delay(cache.get(key)))
        res <- lock.withPermit(fa)
      } yield res
  }
}
