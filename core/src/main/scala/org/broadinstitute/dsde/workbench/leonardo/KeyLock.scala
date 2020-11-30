package org.broadinstitute.dsde.workbench.leonardo

import java.util.concurrent.TimeUnit

import cats.effect.concurrent.Semaphore
import cats.effect.implicits._
import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Effect}
import cats.implicits._
import com.google.common.cache.{CacheBuilder, CacheLoader}

import scala.concurrent.duration.FiniteDuration

abstract class KeyLock[F[_], K] {
  def acquire(key: K): F[Unit]
  def release(key: K): F[Unit]
  def withKeyLock[A](key: K)(fa: F[A]): F[A]
}
object KeyLock {
  def apply[F[_]: ConcurrentEffect: ContextShift, K <: AnyRef](expiryTime: FiniteDuration,
                                                               maxSize: Int,
                                                               blocker: Blocker): F[KeyLock[F, K]] =
    Effect[F].delay(new KeyLockImpl(expiryTime, maxSize, blocker))

  // A KeyLock implementation backed by a guava cache
  final private class KeyLockImpl[F[_]: ConcurrentEffect: ContextShift, K <: AnyRef](expiryTime: FiniteDuration,
                                                                                     maxSize: Int,
                                                                                     blocker: Blocker)
      extends KeyLock[F, K] {

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

    override def acquire(key: K): F[Unit] =
      for {
        lock <- blocker.blockOn(Concurrent[F].delay(cache.get(key)))
        _ <- lock.acquire
      } yield ()

    override def release(key: K): F[Unit] =
      for {
        lock <- blocker.blockOn(Concurrent[F].delay(cache.get(key)))
        _ <- lock.release
      } yield ()

    override def withKeyLock[A](key: K)(fa: F[A]): F[A] =
      for {
        lock <- blocker.blockOn(Concurrent[F].delay(cache.get(key)))
        res <- lock.withPermit(fa)
      } yield res
  }
}
