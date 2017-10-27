package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.google.gcs.GcsBucketName
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.WorkbenchUserServiceAccountEmail
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}

// Mockito Argument Matchers for Value Classes
// for motivation see
// - https://stackoverflow.com/questions/27289757/mockito-matchers-scala-value-class-and-nullpointerexception
// - https://stackoverflow.com/questions/19974493/scala-value-classes-and-mockito-matchers-dont-play-together

trait VCMockitoMatchers {

  def vcAny[T](implicit apply: String => T): T = apply(any[String])

  def vcEq[T](value: T)(implicit apply: String => T, unapply: T => Option[String]): T = apply(mockitoEq(unapply(value).get))

  // need to declare apply for each value class that we want to use with vcAny
  // and also unapply for vcEq

  implicit def gcsbnApply: String => GcsBucketName = GcsBucketName.apply
  implicit def gcsbnUnapply: GcsBucketName => Option[String] = GcsBucketName.unapply

  implicit def gpApply: String => GoogleProject = GoogleProject.apply
  implicit def gpUnapply: GoogleProject => Option[String] = GoogleProject.unapply

  implicit def cnApply: String => ClusterName = ClusterName.apply
  implicit def cnUnapply: ClusterName => Option[String] = ClusterName.unapply

  implicit def onApply: String => OperationName = OperationName.apply
  implicit def onUnapply: OperationName => Option[String] = OperationName.unapply

  implicit def petApply: String => WorkbenchUserServiceAccountEmail = WorkbenchUserServiceAccountEmail.apply
  implicit def petUnapply: WorkbenchUserServiceAccountEmail => Option[String] = WorkbenchUserServiceAccountEmail.unapply
}
