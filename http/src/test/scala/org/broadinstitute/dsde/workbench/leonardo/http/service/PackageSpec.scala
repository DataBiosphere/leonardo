package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
}
