package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import org.broadinstitute.dsde.workbench.leonardo.model.BadRequestException
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class PackageSpec extends AnyFlatSpec with Matchers {
  it should "extract labels to filter by properly" in {
    val input1 = Map("_labels" -> "foo=bar,baz=biz")
    processLabelMap(input1) shouldBe (Right(Map("foo" -> "bar", "baz" -> "biz")))

    val failureInput = Map("_labels" -> "foo=bar,,baz=biz")
    processLabelMap(failureInput).isLeft shouldBe true

    val duplicateLabel = Map("_labels" -> "foo=bar,foo=biz")
    processLabelMap(duplicateLabel) shouldBe (Right(Map("foo" -> "biz")))
  }

  it should "process labels to return properly" in {
    val input1 = "foo,bar"
    processLabelsToReturn(input1) shouldBe Right(List("foo", "bar"))
  }

  it should "process creatorOnly parameter when role=creator" in {
    val email = WorkbenchEmail("user1@example.com")
    val params = Map("role" -> "creator")
    processCreatorOnlyParameter(email, params, TraceId(UUID.randomUUID())) shouldBe Right(Some(email))
  }

  it should "process creatorOnly parameter when creator=caller_email" in {
    val email = WorkbenchEmail("user1@example.com")
    val params = Map("creator" -> email.value)
    processCreatorOnlyParameter(email, params, TraceId(UUID.randomUUID())) shouldBe Right(Some(email))
  }

  it should "process creatorOnly parameter when no role specified" in {
    val email = WorkbenchEmail("user1@example.com")
    val params: Map[String, String] = Map.empty
    processCreatorOnlyParameter(email, params, TraceId(UUID.randomUUID())) shouldBe Right(None)
  }

  it should "reject creatorOnly parameter when role=not_creator" in {
    val email = WorkbenchEmail("user1@example.com")
    val params = Map("role" -> "owner")
    val traceId = TraceId(UUID.randomUUID())
    val res = processCreatorOnlyParameter(email, params, traceId)
    res shouldBe Left(
      BadRequestException("Failed to process invalid value for role. The only currently supported value is creator.",
                          Some(traceId)
      )
    )
  }

  it should "reject creatorOnly parameter when creator=another_email" in {
    val email = WorkbenchEmail("user1@example.com")
    val params = Map("creator" -> "another_user@example.com")
    val traceId = TraceId(UUID.randomUUID())
    val res = processCreatorOnlyParameter(email, params, traceId)
    res shouldBe Left(
      BadRequestException(
        "Failed to process invalid value for creator. The only currently supported value is your own user email.",
        Some(traceId)
      )
    )
  }
}
