package org.broadinstitute.dsde.workbench.leonardo

import org.scalacheck.{Arbitrary, Gen}

object DiskModelGenerators {
  val genDiskSize: Gen[DiskSize] = Gen.chooseNum(10, 500).map(DiskSize)

  implicit val arbDiskSize: Arbitrary[DiskSize] = Arbitrary(genDiskSize)
}
