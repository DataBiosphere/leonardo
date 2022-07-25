package org.broadinstitute.dsde.workbench.leonardo.db

import cats._
import cats.syntax.all._
import slick.basic.BasicBackend
import slick.dbio._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

object DBIOInstances extends DBIOInstances

// This is copied over from https://github.com/RMSone/slick-cats/blob/master/slick-cats/src/main/scala/com/rms/miu/slickcats/DBIOInstances.scala
/**
 * Instances are only provided for `DBIO[A]` and not for `DBIOAction[A, NoStream, Effect.All]`
 * or any other DBIOAction.
 * An implicit `ExecutionContext` must be in scope for the type class conversion to occur.
 * It will otherwise fail with rather unhelpful errors.
 */
trait DBIOInstances extends DBIOInstances0 {
  implicit def dbioInstance(implicit ec: ExecutionContext): MonadError[DBIO, Throwable] with CoflatMap[DBIO] =
    new DBIOCoflatMap with MonadError[DBIO, Throwable] {
      override def pure[A](x: A): DBIO[A] = DBIO.successful(x)

      override def flatMap[A, B](fa: DBIO[A])(f: (A) => DBIO[B]): DBIO[B] = fa.flatMap(f)

      /**
       * While this is roughly the same implementation as in `FutureInstances`,
       * I'm not entirely sure this is indeed stack safe. It certainly looks
       * like it should be.
       */
      override def tailRecM[A, B](a: A)(f: A => DBIO[Either[A, B]]): DBIO[B] =
        f(a).flatMap {
          case Left(a1) => tailRecM(a1)(f)
          case Right(b) => DBIO.successful(b)
        }

      override def handleError[A](fea: DBIO[A])(f: (Throwable) => A): DBIO[A] =
        fea.asTry.map {
          case Success(a) => a
          case Failure(t) => f(t)
        }

      override def raiseError[A](e: Throwable): DBIO[A] = DBIO.failed(e)

      override def map[A, B](fa: DBIO[A])(f: A => B): DBIO[B] = fa.map(f)

      override def handleErrorWith[A](fa: DBIO[A])(f: (Throwable) => DBIO[A]): DBIO[A] =
        fa.asTry.flatMap {
          case Success(a) => DBIO.successful(a)
          case Failure(t) => f(t)
        }
    }

  implicit def dbioGroup[A: Group](implicit ec: ExecutionContext): Group[DBIO[A]] =
    new DBIOGroup[A]

  implicit def dbioMonoid[A: Monoid](implicit ec: ExecutionContext): Monoid[DBIO[A]] =
    new DBIOMonoid[A]

  implicit def dbioSemigroup[A: Semigroup](implicit ec: ExecutionContext): Semigroup[DBIO[A]] =
    new DBIOSemigroup[A]
}

abstract private[db] class DBIOCoflatMap(implicit ec: ExecutionContext) extends CoflatMap[DBIO] {
  def map[A, B](fa: DBIO[A])(f: A => B): DBIO[B] = fa.map(f)
  def coflatMap[A, B](fa: DBIO[A])(f: DBIO[A] => B): DBIO[B] = DBIO.from(Future(f(fa)))
}

private[db] class DBIOSemigroup[A: Semigroup](implicit ec: ExecutionContext) extends Semigroup[DBIO[A]] {
  override def combine(fx: DBIO[A], fy: DBIO[A]): DBIO[A] =
    (fx zip fy).map { case (x, y) => x |+| y }
}

private[db] class DBIOMonoid[A](implicit A: Monoid[A], ec: ExecutionContext)
    extends DBIOSemigroup[A]
    with Monoid[DBIO[A]] {
  def empty: DBIO[A] = DBIO.successful(A.empty)
}

private[db] class DBIOGroup[A](implicit A: Group[A], ec: ExecutionContext) extends DBIOMonoid[A] with Group[DBIO[A]] {
  def inverse(fx: DBIO[A]): DBIO[A] = fx.map(_.inverse())

  override def remove(fx: DBIO[A], fy: DBIO[A]): DBIO[A] =
    (fx zip fy).map { case (x, y) => x |-| y }
}

sealed private[db] trait DBIOInstances0 extends DBIOInstances1 {
  def dbioComonad(atMost: FiniteDuration, db: BasicBackend#DatabaseDef)(implicit ec: ExecutionContext): Comonad[DBIO] =
    new DBIOCoflatMap with Comonad[DBIO] {
      def extract[A](x: DBIO[A]): A =
        Await.result(db.run(x), atMost)
    }

  def dbioOrder[A: Order](atMost: FiniteDuration, db: BasicBackend#DatabaseDef)(implicit
    ec: ExecutionContext
  ): Order[DBIO[A]] =
    new Order[DBIO[A]] {
      def compare(x: DBIO[A], y: DBIO[A]): Int =
        Await.result(db.run((x zip y).map { case (a, b) => a compare b }), atMost)
    }
}

sealed private[db] trait DBIOInstances1 extends DBIOInstances2 {
  def dbioPartialOrder[A: PartialOrder](atMost: FiniteDuration, db: BasicBackend#DatabaseDef)(implicit
    ec: ExecutionContext
  ): PartialOrder[DBIO[A]] =
    new PartialOrder[DBIO[A]] {
      def partialCompare(x: DBIO[A], y: DBIO[A]): Double =
        Await.result(db.run((x zip y).map { case (a, b) => a partialCompare b }), atMost)
    }
}

sealed private[db] trait DBIOInstances2 {
  def dbioEq[A: Eq](atMost: FiniteDuration, db: BasicBackend#DatabaseDef)(implicit ec: ExecutionContext): Eq[DBIO[A]] =
    new Eq[DBIO[A]] {
      def eqv(x: DBIO[A], y: DBIO[A]): Boolean =
        Await.result(db.run((x zip y).map { case (a, b) => a === b }), atMost)
    }
}
