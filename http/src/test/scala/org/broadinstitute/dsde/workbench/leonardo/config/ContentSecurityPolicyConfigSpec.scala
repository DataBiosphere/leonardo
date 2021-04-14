package org.broadinstitute.dsde.workbench.leonardo
package config

import org.broadinstitute.dsde.workbench.leonardo.config.ContentSecurityPolicyComponent._
import org.scalatest.flatspec.AnyFlatSpecLike

class ContentSecurityPolicyConfigSpec extends LeonardoTestSuite with AnyFlatSpecLike {

  "ContentSecurityPolicyConfig" should "generate a valid string" in {
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
          "data:",
          "'unsafe-inline'",
          "'unsafe-eval'",
          "https://apis.google.com",
          "https://cdn.jsdelivr.net",
          "https://cdn.pydata.org"
        )
      ),
      StyleSrc(
        List(
          "'self'",
          "'unsafe-inline'",
          "data:",
          "https://cdn.pydata.org"
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
          "https://portals.broadinstitute.org/webservices/igv/",
          "https://igv.org/genomes/",
          "https://raw.githubusercontent.com/PAIR-code/facets/1.0.0/facets-dist/facets-jupyter.html",
          "https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js"
        )
      ),
      ObjectSrc(
        List(
          "'none'"
        )
      ),
      ReportUri(
        List(
          "https://terra.report-uri.com/r/d/csp/reportOnly"
        )
      )
    )

    test.asString shouldBe
      "frame-ancestors 'self' *.terra.bio https://bvdp-saturn-prod.appspot.com https://all-of-us-rw-staging.appspot.com https://all-of-us-rw-stable.appspot.com https://stable.fake-research-aou.org https://workbench.researchallofus.org terra.biodatacatalyst.nhlbi.nih.gov; script-src 'self' data: 'unsafe-inline' 'unsafe-eval' https://apis.google.com https://cdn.jsdelivr.net https://cdn.pydata.org; style-src 'self' 'unsafe-inline' data: https://cdn.pydata.org; connect-src 'self' wss://*.broadinstitute.org:* wss://notebooks.firecloud.org:* *.googleapis.com https://*.npmjs.org https://data.broadinstitute.org https://s3.amazonaws.com/igv.broadinstitute.org/ https://s3.amazonaws.com/igv.org.genomes/ https://portals.broadinstitute.org/webservices/igv/ https://igv.org/genomes/ https://raw.githubusercontent.com/PAIR-code/facets/1.0.0/facets-dist/facets-jupyter.html https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js; object-src 'none'; report-uri https://terra.report-uri.com/r/d/csp/reportOnly"

  }

  it should "parse config values correctly" in {
    CommonTestData.contentSecurityPolicy shouldBe
      "frame-ancestors 'none'; script-src 'self' data: 'unsafe-inline' 'unsafe-eval' https://apis.google.com https://cdn.jsdelivr.net https://cdn.pydata.org; style-src 'self' 'unsafe-inline' data: https://cdn.pydata.org; connect-src 'self' wss://*.broadinstitute.org:* wss://notebooks.firecloud.org:* *.googleapis.com https://*.npmjs.org https://data.broadinstitute.org https://s3.amazonaws.com/igv.broadinstitute.org/ https://s3.amazonaws.com/igv.org.genomes/ https://portals.broadinstitute.org/webservices/igv/ https://igv.org/genomes/ https://raw.githubusercontent.com/PAIR-code/facets/1.0.0/facets-dist/facets-jupyter.html https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js; object-src 'none'; report-uri https://terra.report-uri.com/r/d/csp/reportOnly"
  }

}
