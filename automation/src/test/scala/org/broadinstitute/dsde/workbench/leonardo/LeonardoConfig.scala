package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.auth.{AuthTokenScopes, UserAuthToken}
import org.broadinstitute.dsde.workbench.config.{CommonConfig, Credentials}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.FreeSpec

object LeonardoConfig extends CommonConfig {
  private val leonardo = config.getConfig("leonardo")
  private val gcs = config.getConfig("gcs")

  object Leonardo {
    val apiUrl: String = leonardo.getString("apiUrl")
    val notebooksServiceAccountEmail: String = leonardo.getString("notebooksServiceAccountEmail")
  }

  // for qaEmail and pathToQAPem and pathToQAJson
  object GCS extends CommonGCS {
    val pathToQAJson = gcs.getString("qaJsonFile")
  }

  // for NotebooksWhitelisted
  object Users extends CommonUsers

  // Hack: FlatSpec is mixed in to adhere to BillingFixtures self-type. This object does not actually define any specs.
  object BillingProject extends FreeSpec with BillingFixtures {
    val ronCreds: Credentials = Users.NotebooksWhitelisted.getUserCredential("ron")
    val hermioneCreds: Credentials = Users.NotebooksWhitelisted.getUserCredential("hermione")

    val ronAuthToken = UserAuthToken(ronCreds, AuthTokenScopes.userLoginScopes)
    val hermioneAuthToken = UserAuthToken(hermioneCreds, AuthTokenScopes.userLoginScopes)

    // TODO: this has the disadvantage of never unclaiming the project from gpalloc. Might need to find a new strategy.
    val project: GoogleProject = {
      try {
        logger.info(s"Claiming a billing project from gpalloc")
        val claimedBillingProject = claimGPAllocProject(hermioneCreds)
        logger.info(s"Billing project claimed: ${claimedBillingProject.projectName}")
        GoogleProject(claimedBillingProject.projectName)
      } catch {
        case ex: Exception =>
          logger.error(s"ERROR: when owner $hermioneCreds is claiming a billing project", ex)
          throw ex
      }
    }
  }
}
