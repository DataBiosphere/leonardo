package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.workbench.leonardo.config.SwaggerConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbSingleton
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, ProxyService}

/**
  * Created by rtitle on 8/15/17.
  */
trait TestLeoRoutes { this: ScalatestRouteTest =>
  val mockGoogleDataprocDAO = new MockGoogleDataprocDAO
  val leonardoService = new LeonardoService(mockGoogleDataprocDAO, DbSingleton.ref)
  val proxyService = new ProxyService(DbSingleton.ref)
  val swaggerConfig = SwaggerConfig()
  val leoRoutes = new LeoRoutes(leonardoService, proxyService, swaggerConfig)
}
