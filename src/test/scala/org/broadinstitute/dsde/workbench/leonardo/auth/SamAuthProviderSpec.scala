package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class SamAuthProviderSpec extends FlatSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll with TestComponent with ScalaFutures with OptionValues {{
  private val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  private val serviceAccountProvider = new MockPetsPerProjectServiceAccountProvider(config.getConfig("serviceAccounts.config"))
  private val samAuthProvider = new SamAuthProvider(config.getConfig("auth.samAuthProviderConfig"), serviceAccountProvider)


}
