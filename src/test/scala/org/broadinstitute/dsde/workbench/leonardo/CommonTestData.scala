package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.google.gcs.{GcsBucketName, GcsPath, GcsRelativePath}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.WorkbenchUserServiceAccountEmail

// values common to multiple tests, to reduce boilerplate

trait CommonTestData {
  val name1 = ClusterName("name1")
  val name2 = ClusterName("name2")
  val name3 = ClusterName("name3")
  val project = GoogleProject("dsp-leo-test")
  val googleServiceAccount = WorkbenchUserServiceAccountEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val jupyterExtensionUri = Some(GcsPath(GcsBucketName("extension_bucket"), GcsRelativePath("extension_path")))
}

trait GcsPathUtils {
  def gcsPath(str: String): GcsPath = {
    GcsPath.parse(str).right.get
  }
}
