package org.broadinstitute.dsde.workbench.leonardo.provider

import org.broadinstitute.dsde.workbench.leonardo.http.service.StatusService
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus, Subsystems}
import org.mockito.Mockito.when
import org.mockito.stubbing.OngoingStubbing
import pact4s.provider._

import scala.concurrent.Future

object StatusStateManager {
  object States {
    final val SystemOk = "the system is ok"
    final val SystemDown = "the system is down"
  }
  private def mockGetStatus(mockStatusService: StatusService,
                            mockResponse: Future[StatusCheckResponse]
  ): OngoingStubbing[Future[StatusCheckResponse]] =
    when {
      mockStatusService.getStatus()
    } thenReturn {
      mockResponse
    }

  def handler(mockStatusService: StatusService): PartialFunction[ProviderState, Unit] = {
    case ProviderState(States.SystemOk, _) =>
      mockGetStatus(mockStatusService,
                    Future.successful(
                      StatusCheckResponse(ok = true,
                                          Map(
                                            Subsystems.Cromwell -> SubsystemStatus(ok = true, Some(List("Looks good")))
                                          )
                      )
                    )
      )
    case ProviderState(States.SystemDown, _) =>
      mockGetStatus(
        mockStatusService,
        Future.successful(
          StatusCheckResponse(ok = false,
                              Map(
                                Subsystems.Cromwell -> SubsystemStatus(ok = false, Some(List("System is down")))
                              )
          )
        )
      )
  }
}
