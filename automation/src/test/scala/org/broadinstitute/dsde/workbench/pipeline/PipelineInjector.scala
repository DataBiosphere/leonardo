package org.broadinstitute.dsde.workbench.pipeline

import com.typesafe.scalalogging.LazyLogging
import io.circe.parser.decode

import java.util.Base64
import scala.util.Random

object PredefinedEnv {
  val BillingProject: String = "BILLING_PROJECT"
  val UsersMetadataB64: String = "USERS_METADATA_B64"
  val E2EENV: String = "E2E_ENV"
}

trait PipelineInjector {
  // The name of the environment you requested the pipeline to return.
  def environmentName: String

  // Returns the billing project name you requested the pipeline to create.
  def billingProject: String =
    sys.env.getOrElse(PredefinedEnv.BillingProject, "")

  // Retrieves user metadata from the environment and decodes it from Base64.
  // Returns a sequence of UserMetadata objects. An empty Seq will be returned if retrieval fails.
  def usersMetadata: Seq[UserMetadata] =
    sys.env.get(PredefinedEnv.UsersMetadataB64) match {
      case Some(b64) =>
        val decoded = decode[Seq[UserMetadata]](new String(Base64.getDecoder.decode(b64), "UTF-8"))
        decoded match {
          case Right(u)    => u
          case Left(error) => Seq()
        }
      case _ => Seq()
    }

  trait Users {
    val users: Seq[UserMetadata]

    def getUserCredential(like: String): Option[UserMetadata] = {
      val filteredResults = users.filter(_.email.toLowerCase.contains(like.toLowerCase))
      if (filteredResults.isEmpty) None else Some(filteredResults.head)
    }
  }

  object Owners extends Users {
    val users: Seq[UserMetadata] = usersMetadata.filter(_.`type` == Owner)
  }

  object Students extends Users {
    val users: Seq[UserMetadata] = usersMetadata.filter(_.`type` == Student)
  }

  def chooseStudent: Option[UserMetadata] = {
    val students = usersMetadata.filter(_.`type` == Student)
    if (students.isEmpty) None else Some(students(Random.nextInt(students.length)))
  }
}

object PipelineInjector extends LazyLogging {
  def apply(envName: String): PipelineInjector = new PipelineInjector {
    override val environmentName: String = envName
  }

  def e2eEnv(): String = {
    logger.debug("E2E Env: " + sys.env.getOrElse(PredefinedEnv.E2EENV, ""))
    sys.env.getOrElse(PredefinedEnv.E2EENV, "")
  }
}
