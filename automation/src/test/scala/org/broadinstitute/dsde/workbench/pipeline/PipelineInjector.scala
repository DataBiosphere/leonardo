package org.broadinstitute.dsde.workbench.pipeline

import com.typesafe.scalalogging.LazyLogging
import io.circe.parser

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

  def usersMetadata: Seq[UserMetadata] =
    sys.env.get(PredefinedEnv.UsersMetadataB64) match {
      case Some(b64) =>
        val decodedB64 = new String(Base64.getDecoder.decode(b64), "UTF-8")
        val userMetadataSeq = for {
          json <- parser.parse(decodedB64)
          seq <- json.as[Seq[UserMetadata]]
        } yield seq
        userMetadataSeq match {
          case Right(u) => u
          case Left(_)  => Seq()
        }
      case _ => Seq()
    }

  trait Users {
    val users: Seq[UserMetadata]

    def getUserCredential(like: String): Option[UserMetadata] = {
      println("getUserCredential: " + like)
      println("users: " + users)
      users.find(_.email.toLowerCase.contains(like.toLowerCase))
    }
  }

  object Owners extends Users {
    println("Owners filter")
    println(usersMetadata)
    val users: Seq[UserMetadata] = usersMetadata.filter(_.`type` == Owner)
    println(users)
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

object Pipeline {
  val BEE: PipelineInjector = PipelineInjector(PipelineInjector.e2eEnv())
  val BILLING_PROJECT: String = BEE.billingProject
}
