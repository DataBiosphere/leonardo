package org.broadinstitute.dsde.workbench.leonardo

import java.util.concurrent.TimeUnit

import cats.effect.concurrent.{MVar, MVar2}
import cats.effect.implicits._
import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Effect}
import cats.implicits._
import com.google.common.cache.{CacheBuilder, CacheLoader}

import scala.concurrent.duration.FiniteDuration

abstract class Lock[F[_]] {
  def acquire: F[Unit]
  def release: F[Unit]
  def withLock[A](fa: F[A]): F[A]
}
object Lock {
  def apply[F[_]: Concurrent]: F[Lock[F]] =
    MVar.of(()).map(l => new LockImpl(l))

  // A Lock implementation backed by an MVar
  // Inspired from https://typelevel.org/cats-effect/concurrency/mvar.html#use-case-asynchronous-lock-binary-semaphore-mutex
  final private class LockImpl[F[_]: Concurrent](lock: MVar2[F, Unit]) extends Lock[F] {
    def acquire: F[Unit] = lock.take
    def release: F[Unit] = lock.put(())
    def withLock[A](fa: F[A]): F[A] =
      acquire.bracket(_ => fa)(_ => release)
  }
}

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
        new CacheLoader[K, Lock[F]] {
          def load(key: K): Lock[F] =
            Lock[F].toIO.unsafeRunSync()
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
        res <- lock.withLock(fa)
      } yield res
  }
}
