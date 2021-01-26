package org.broadinstitute.dsde.workbench.leonardo.http

import com.google.pubsub.v1.{ProjectSubscriptionName, ProjectTopicName, TopicName}
import org.broadinstitute.dsde.workbench.google2.{
  MaxRetries,
  PublisherConfig,
  SubscriberConfig,
  SubscriberDeadLetterPolicy
}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestSuite
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Paths
import scala.concurrent.duration._
import org.broadinstitute.dsde.workbench.leonardo.Equalities.subscriberConfigEquality

class NewConfigSpec extends AnyFlatSpec with LeonardoTestSuite {
  implicit val appConfigEquality = {
    new Equality[HttpAppConfig] {
      def areEqual(a: HttpAppConfig, b: Any): Boolean =
        b match {
          case c: HttpAppConfig =>
            a.copy(nonLeonardoMessageSubscriber = null) == c
              .copy(nonLeonardoMessageSubscriber = null) &&
              (a.nonLeonardoMessageSubscriber === c.nonLeonardoMessageSubscriber)
          case _ => false
        }
    }
  }

  it should "read config properly" in {
    val project = GoogleProject("leo-project")
    val leoSa = Paths.get("leo-account.json")

    val expectedAppConfig = HttpAppConfig(
      PublisherConfig(leoSa.toString, ProjectTopicName.of(project.value, "terra-cryptomining")),
      SubscriberConfig(
        leoSa.toString,
        TopicName.of(project.value, "replace-me"),
        Some(ProjectSubscriptionName.of(project.value, "replace-me")),
        5 minutes,
        Some(SubscriberDeadLetterPolicy(TopicName.of(project.value, "leoDeadLetterTopic"), MaxRetries(5))),
        None,
        Some("NOT attributes:leonardo")
      )
    )
    (NewConfig.appConfig.toOption.get === expectedAppConfig) shouldBe (true)
  }
}
