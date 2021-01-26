package org.broadinstitute.dsde.workbench.leonardo

import akka.actor.ActorSystem
import cats.effect.IO
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{traceId, userInfo}
import org.broadinstitute.dsde.workbench.leonardo.auth.{MockPetClusterServiceAccountProvider, WhitelistAuthProvider}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import javax.net.ssl.SSLContext
import org.scalatest.matchers.should.Matchers

object TestUtils extends Matchers {
  def sslContext(implicit as: ActorSystem): SSLContext = SslContextReader.getSSLContext[IO]().unsafeRunSync()

  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load()).resolve()
  val whitelistAuthConfig = config.getConfig("auth.whitelistProviderConfig")

  val serviceAccountProvider = new MockPetClusterServiceAccountProvider
  val whitelistAuthProvider = new WhitelistAuthProvider(whitelistAuthConfig, serviceAccountProvider)

  def clusterServiceAccountFromProject(googleProject: GoogleProject): Option[WorkbenchEmail] =
    serviceAccountProvider.getClusterServiceAccount(userInfo, googleProject)(traceId).unsafeRunSync()

  def notebookServiceAccountFromProject(googleProject: GoogleProject): Option[WorkbenchEmail] =
    serviceAccountProvider.getNotebookServiceAccount(userInfo, googleProject)(traceId).unsafeRunSync()
}
