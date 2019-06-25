package org.broadinstitute.dsde.workbench.leonardo.notebooks

import java.io.File

import org.scalatest.{FreeSpec, ParallelTestExecution}
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google.GcsEntityTypes.Group
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.Reader
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsObjectName, GcsPath, parseGcsPath}
import org.broadinstitute.dsde.workbench.service.Sam
import org.broadinstitute.dsde.workbench.dao.Google.{googleIamDAO, googleStorageDAO}
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath}
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.broadinstitute.dsde.workbench.service.RestException
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.V2

import scala.language.postfixOps

import scala.concurrent.duration._
import org.scalatest.time.{Seconds, Span}


class NotebookExtensionSpec extends FreeSpec with NotebookTestUtils with ParallelTestExecution with BillingFixtures  {


  "Leonardo notebooks" - {

    "welder should be up" in {
      withProject { project =>
        implicit token =>
          withNewCluster(project, apiVersion = V2) { cluster =>
//            withWebDriver { implicit driver =>
            println("here")
              Welder.getWelderStatus(cluster)
              "ok"
//            }
          }
      }
    }

  }

}
