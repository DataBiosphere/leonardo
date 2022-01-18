package org.broadinstitute.dsde.workbench
package leonardo
package util

import cats.effect.Async
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpWsmDao
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext

class AzureInterpreter[F[_]](config: _,
                             wsmDao: HttpWsmDao[F])
                            (implicit val executionContext: ExecutionContext, logger: StructuredLogger[F], dbRef: DbReference[F], F: Async[F]) extends AzureAlgebra[F] {
  /** Creates an Azure VM but doesn't wait for its completion.
   * This includes creation of all child Azure resources (disk, network, ip), and assumes these are created syncronously
   * */
  override def createRuntime(params: CreateAzureRuntimeParams)(implicit ev: Ask[F, AppContext]): F[CreateAzureRuntimeResult] =
    for {
      _ <- wsmDao.createIp()
      _ <- wsmDao.createDisk()
      _ <- wsmDao.createNetwork()

    }

  /**
   * Polls a creating an Azure VM for its completion
   * Also [TBD]
   */
  override def pollRuntime(params: PollRuntimeParams)(implicit ev: Ask[F, AppContext]): F[Unit] = ???
}
