package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, LeonardoTestSuite}
import org.broadinstitute.dsde.workbench.leonardo.config.ContentSecurityPolicyComponent.{
  ConnectSrc,
  FrameAncestors,
  ObjectSrc,
  ScriptSrc,
  StyleSrc
}
import org.scalatest.FlatSpecLike

class ContentSecurityPolicyConfigSpec extends LeonardoTestSuite with FlatSpecLike {

  it should "generate a valid string" in {
    val test = ContentSecurityPolicyConfig(
      FrameAncestors(
        List(
          "'self'",
          "*.terra.bio",
          "https://bvdp-saturn-prod.appspot.com",
          "https://all-of-us-rw-staging.appspot.com",
          "https://all-of-us-rw-stable.appspot.com",
          "https://stable.fake-research-aou.org",
          "https://workbench.researchallofus.org",
          "terra.biodatacatalyst.nhlbi.nih.gov"
        )
      ),
      ScriptSrc(
        List(
          "'self'",
          "data:text/javascript",
          "'unsafe-inline'",
          "'unsafe-eval'",
          "https://apis.google.com"
        )
      ),
      StyleSrc(
        List(
          "'self'",
          "'unsafe-inline'"
        )
      ),
      ConnectSrc(
        List(
          "'self'",
          "wss://*.broadinstitute.org:*",
          "wss://notebooks.firecloud.org:*",
          "*.googleapis.com",
          "https://*.npmjs.org",
          "https://data.broadinstitute.org",
          "https://s3.amazonaws.com/igv.broadinstitute.org/",
          "https://s3.amazonaws.com/igv.org.genomes/",
          "https://raw.githubusercontent.com/PAIR-code/facets/1.0.0/facets-dist/facets-jupyter.html",
          "https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js"
        )
      ),
      ObjectSrc(
        List(
          "'none'"
        )
      )
    )

    test.asString shouldBe
      "frame-ancestors 'self' *.terra.bio https://bvdp-saturn-prod.appspot.com https://all-of-us-rw-staging.appspot.com https://all-of-us-rw-stable.appspot.com https://stable.fake-research-aou.org https://workbench.researchallofus.org terra.biodatacatalyst.nhlbi.nih.gov; script-src 'self' data:text/javascript 'unsafe-inline' 'unsafe-eval' https://apis.google.com; style-src 'self' 'unsafe-inline'; connect-src 'self' wss://*.broadinstitute.org:* wss://notebooks.firecloud.org:* *.googleapis.com https://*.npmjs.org https://data.broadinstitute.org https://s3.amazonaws.com/igv.broadinstitute.org/ https://s3.amazonaws.com/igv.org.genomes/ https://raw.githubusercontent.com/PAIR-code/facets/1.0.0/facets-dist/facets-jupyter.html https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js; object-src 'none'"

  }

  it should "parse config values correctly" in {
    CommonTestData.contentSecurityPolicy shouldBe
      "frame-ancestors 'none'; script-src 'self' data:text/javascript 'unsafe-inline' 'unsafe-eval' https://apis.google.com; style-src 'self' 'unsafe-inline'; connect-src 'self' wss://*.broadinstitute.org:* wss://notebooks.firecloud.org:* *.googleapis.com https://*.npmjs.org https://data.broadinstitute.org https://s3.amazonaws.com/igv.broadinstitute.org/ https://s3.amazonaws.com/igv.org.genomes/ https://raw.githubusercontent.com/PAIR-code/facets/1.0.0/facets-dist/facets-jupyter.html https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js; object-src 'none'"
  }

}
