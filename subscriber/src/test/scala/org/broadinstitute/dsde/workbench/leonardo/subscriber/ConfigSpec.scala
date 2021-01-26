package org.broadinstitute.dsde.workbench.leonardo
package subscriber

import cats.syntax.all._
import com.google.pubsub.v1.{ProjectSubscriptionName, ProjectTopicName, TopicName}
import org.broadinstitute.dsde.workbench.google2.{
  MaxRetries,
  PublisherConfig,
  SubscriberConfig,
  SubscriberDeadLetterPolicy
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{
  appMonitorConfig,
  galaxyAppConfig,
  galaxyDiskConfig,
  kubernetesClusterConfig,
  kubernetesIngressConfig,
  testProxyConfig,
  testSecurityFilesConfig,
  vpcConfig
}
import org.broadinstitute.dsde.workbench.leonardo.algebra.HttpSamDaoConfig
import org.broadinstitute.dsde.workbench.leonardo.db.LiquibaseConfig
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.Uri
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.broadinstitute.dsde.workbench.leonardo.Equalities.subscriberConfigEquality
import java.nio.file.Paths
import scala.concurrent.duration._

class ConfigSpec extends AnyFlatSpec with LeonardoTestSuite {
  implicit val appConfigEquality = {
    new Equality[AppConfig] {
      def areEqual(a: AppConfig, b: Any): Boolean =
        b match {
          case c: AppConfig =>
            a.copy(nonLeonardoMessageSubscriber = null) == c
              .copy(nonLeonardoMessageSubscriber = null) && a.nonLeonardoMessageSubscriber === c.nonLeonardoMessageSubscriber
          case _ => false
        }
    }
  }

  it should "read config properly" in {
    val project = GoogleProject("fake-project")
    val leoSa = Paths.get("fake-sa")

    val expectedAppConfig = AppConfig(
      Application(leoSa, "leonardo", project),
      DbConfig(LiquibaseConfig("org/broadinstitute/dsde/workbench/leonardo/liquibase/changelog.xml", true), 20),
      HttpSamDaoConfig(Uri.unsafeFromString("https://sam.com"), true, 60 minutes, 1000, leoSa),
      PublisherConfig(leoSa.toString, ProjectTopicName.of(project.value, "terra-cryptomining")),
      AsyncTaskProcessor.Config(500, 200),
      SubscriberConfig(
        leoSa.toString,
        TopicName.of(project.value, "fake-leo-topic"),
        Some(ProjectSubscriptionName.of(project.value, "fake-subscription")),
        5 minutes,
        Some(SubscriberDeadLetterPolicy(TopicName.of(project.value, "leoDeadLetterTopic"), MaxRetries(5))),
        None,
        Some("NOT attributes:leonardo")
      ),
      GkeConfig(
        kubernetesClusterConfig,
        galaxyAppConfig,
        kubernetesIngressConfig,
        galaxyDiskConfig
      ),
      vpcConfig,
      testProxyConfig,
      DnsCacheConfig(
        CacheConfig(
          5 seconds,
          10000
        )
      ),
      testSecurityFilesConfig,
      MonitorConfig(
        appMonitorConfig
      )
    )
    // have to use Equality because `TopicName`'s default equality check doesn't work
    (Config.appConfig.toOption.get === expectedAppConfig) shouldBe (true)
  }
}
