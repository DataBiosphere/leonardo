package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.util.toScalaDuration
import java.time.Duration

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 12/5/17.
  */
abstract class SamServiceAccountProvider(config: Config) extends ServiceAccountProvider(config) {
  // Need to specify a new ActorSystem for Sam
  implicit val system = ActorSystem("SamServiceAccountProvider")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  protected lazy val samServer = config.as[String]("samServer")
  protected lazy val cacheExpiryTime = toScalaDuration(Option(config.getDuration("samCacheExpiryTime")).getOrElse(Duration.ofMinutes(60)))
  protected lazy val cacheMaxSize = Option(config.as[Int]("samCacheMaxSize")).getOrElse(1000)
  protected lazy val (leoEmail, leoPemFile) = getLeoServiceAccountAndKey
  protected lazy val samDAO = new HttpSamDAO(samServer)
  protected lazy val samClient = new SwaggerSamClient(samServer, cacheExpiryTime, cacheMaxSize, leoEmail, leoPemFile)

  override def listUsersStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    Future.successful(List.empty[WorkbenchEmail])
  }

  override def listGroupsStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    Future(samClient.getUserProxyFromSam(userEmail)).map(List(_))
  }

}
