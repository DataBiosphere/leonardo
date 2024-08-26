package org.broadinstitute.dsde.workbench.leonardo
package util

import _root_.org.typelevel.log4cats.Logger
import cats.data.NonEmptyList
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.cloud.Identity
import fs2._
import fs2.io.file.Files
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates.{standardGoogleRetryPredicate, whenStatusCode}
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService, StorageRole}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.sam.SamService
import org.broadinstitute.dsde.workbench.leonardo.model.LeoInternalServerError
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, ServiceAccountKey}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}

import java.nio.charset.StandardCharsets
import scala.io.Source

class BucketHelper[F[_]](
  config: BucketHelperConfig,
  google2StorageDAO: GoogleStorageService[F],
  samService: SamService[F]
)(implicit val logger: Logger[F], F: Async[F], files: Files[F]) {

  val leoEntity = serviceAccountIdentity(Config.applicationConfig.leoServiceAccountEmail)

  /**
   * Creates the dataproc init bucket and sets the necessary ACLs.
   */
  def createInitBucket(googleProject: GoogleProject, bucketName: GcsBucketName, serviceAccount: WorkbenchEmail)(implicit
    ev: Ask[F, AppContext]
  ): Stream[F, Unit] =
    for {
      ctx <- Stream.eval(ev.ask)
      // The init bucket is created in the cluster's project.
      // Leo service account -> Owner
      // available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Reader
      bucketSAs <- getBucketSAs(serviceAccount)
      readerAcl = NonEmptyList
        .fromList(bucketSAs)
        .map(readers => Map(StorageRole.ObjectViewer -> readers))
        .getOrElse(Map.empty)
      ownerAcl = Map(StorageRole.ObjectAdmin -> NonEmptyList.one(leoEntity))

      _ <- google2StorageDAO.insertBucket(googleProject, bucketName, traceId = Some(ctx.traceId))
      _ <- google2StorageDAO.setIamPolicy(bucketName, (readerAcl ++ ownerAcl).toMap, traceId = Some(ctx.traceId))
    } yield ()

  /**
   * Creates the dataproc staging bucket and sets the necessary ACLs.
   */
  def createStagingBucket(
    userEmail: WorkbenchEmail,
    googleProject: GoogleProject,
    bucketName: GcsBucketName,
    serviceAccountInfo: WorkbenchEmail
  )(implicit ev: Ask[F, AppContext]): Stream[F, Unit] =
    for {
      ctx <- Stream.eval(ev.ask)
      // The staging bucket is created in the cluster's project.
      // Leo service account -> Owner
      // Available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Owner
      // User proxy group -> Reader
      bucketSAs <- getBucketSAs(serviceAccountInfo)
      proxyGroup <- Stream.eval(samService.getProxyGroup(userEmail)).map(groupIdentity)

      readerAcl = Map(StorageRole.ObjectViewer -> NonEmptyList.one(proxyGroup))
      ownerAcl = Map(StorageRole.ObjectAdmin -> NonEmptyList(leoEntity, bucketSAs))

      _ <- google2StorageDAO.insertBucket(googleProject, bucketName, traceId = Some(ctx.traceId))
      // sometimes GCP will throw 404 when setIamPolicy happens too soon
      retryConfig = RetryPredicates.retryConfigWithPredicates(
        RetryPredicates.combine(List(standardGoogleRetryPredicate, whenStatusCode(404)))
      )
      _ <- google2StorageDAO.setIamPolicy(bucketName,
                                          (readerAcl ++ ownerAcl).toMap,
                                          retryConfig = retryConfig,
                                          traceId = Some(ctx.traceId)
      )
    } yield ()

  def deleteInitBucket(googleProject: GoogleProject, initBucketName: GcsBucketName)(implicit
    ev: Ask[F, TraceId]
  ): F[Unit] =
    for {
      traceId <- ev.ask
      _ <- google2StorageDAO
        .deleteBucket(googleProject, initBucketName, isRecursive = true, traceId = Some(traceId))
        .compile
        .drain
    } yield ()

  def uploadFileToInitBucket(initBucketName: GcsBucketName, runtimeResource: RuntimeResource): F[Unit] =
    (TemplateHelper.resourceStream[F](runtimeResource) through google2StorageDAO
      .streamUploadBlob(
        initBucketName,
        GcsBlobName(runtimeResource.asString)
      )).compile.drain

  def uploadClusterCertsToInitBucket(initBucketName: GcsBucketName): F[Unit] = {
    val uploadStream = for {
      f <- Stream.emits(
        Seq(
          config.clusterFilesConfig.proxyServerCrt,
          config.clusterFilesConfig.proxyServerKey,
          config.clusterFilesConfig.proxyRootCaPem
        )
      )
      upload <- TemplateHelper.fileStream[F](f) through google2StorageDAO.streamUploadBlob(
        initBucketName,
        GcsBlobName(f.getFileName.toString)
      )
    } yield upload
    uploadStream.compile.drain
  }

  def initializeBucketObjects(
    initBucketName: GcsBucketName,
    serviceAccountKey: Option[ServiceAccountKey],
    templateValues: RuntimeTemplateValues,
    customClusterEnvironmentVariables: Map[String, String],
    clusterResourcesConfig: ClusterResourcesConfig,
    gpuConfig: Option[GpuConfig]
  )(implicit ev: Ask[F, AppContext]): Stream[F, Unit] = {
    // Build a mapping of (name, value) pairs with which to apply templating logic to resources
    val replacements = templateValues.toMap

    // Jupyter allows setting of arbitrary environment variables on cluster creation if they are passed in to
    // docker-compose as a file of format:
    //     var1=value1
    //     var2=value2
    // etc. We're building a string of that format here.
    val customEnvVars = customClusterEnvironmentVariables.foldLeft("") { case (memo, (key, value)) =>
      memo + s"$key=$value\n"
    }
    val uploadRawFiles = for {
      f <- Stream.emits(
        Seq(
          config.clusterFilesConfig.proxyServerCrt,
          config.clusterFilesConfig.proxyServerKey,
          config.clusterFilesConfig.proxyRootCaPem
        )
      )
      _ <- TemplateHelper.fileStream[F](f) through google2StorageDAO.streamUploadBlob(
        initBucketName,
        GcsBlobName(f.getFileName.toString)
      )
    } yield ()

    val uploadRawResources =
      Stream
        .emits(
          List(
            clusterResourcesConfig.jupyterDockerCompose,
            clusterResourcesConfig.rstudioDockerCompose,
            clusterResourcesConfig.proxyDockerCompose,
            clusterResourcesConfig.proxySiteConf,
            clusterResourcesConfig.welderDockerCompose
          )
        )
        .covary[F]
        .evalMap { r =>
          (TemplateHelper.resourceStream[F](r) through google2StorageDAO.streamUploadBlob(
            initBucketName,
            GcsBlobName(r.asString)
          )).compile.drain
        }

    val uploadGpuDockerCompose = gpuConfig.traverse { gC =>
      val additionalGpuConfigString = (1 to gC.numOfGpus)
        .map(x => s"""
                     |      - "/dev/nvidia${x - 1}:/dev/nvidia${x - 1}"""".stripMargin)
        .mkString("", "", "\n")

      for {
        ctx <- ev.ask
        gpuDockerCompose <- F.fromEither(
          clusterResourcesConfig.gpuDockerCompose.toRight(
            LeoInternalServerError(
              "This is impossible. If GPU config is defined, then gpuDockerCompose should be defined as well",
              Some(ctx.traceId)
            )
          )
        )
        sourceStream = (Stream
          .fromIterator(
            Source
              .fromResource(s"${ClusterResourcesConfig.basePath}/${gpuDockerCompose.asString}")
              .getLines(),
            1024
          )
          .intersperse("\n") ++ Stream.emit(additionalGpuConfigString).covary[F]).flatMap(s =>
          Stream.emits(s.getBytes(java.nio.charset.Charset.forName("UTF-8"))).covary[F]
        )
        _ <- (sourceStream through google2StorageDAO.streamUploadBlob(
          initBucketName,
          GcsBlobName(gpuDockerCompose.asString)
        )).compile.drain
      } yield ()
    }.void

    val uploadTemplatedResources =
      Stream
        .emits(
          List(
            clusterResourcesConfig.initScript,
            clusterResourcesConfig.jupyterNotebookFrontendConfigUri
          )
        )
        .evalMap { r =>
          (TemplateHelper.templateResource[F](replacements, r) through google2StorageDAO.streamUploadBlob(
            initBucketName,
            GcsBlobName(r.asString)
          )).compile.drain
        }

    val uploadPrivateKey = for {
      k <- Stream(serviceAccountKey).unNone
      data <- Stream(k.privateKeyData.decode).unNone
      _ <- Stream.emits(data.getBytes(StandardCharsets.UTF_8)) through google2StorageDAO.streamUploadBlob(
        initBucketName,
        GcsBlobName(RuntimeTemplateValues.serviceAccountCredentialsFilename)
      )
    } yield ()

    val uploadCustomEnvVars = Stream.emits(customEnvVars.getBytes(StandardCharsets.UTF_8)) through google2StorageDAO
      .streamUploadBlob(initBucketName, GcsBlobName(clusterResourcesConfig.customEnvVarsConfigUri.asString))

    Stream(uploadRawFiles,
           uploadRawResources,
           uploadTemplatedResources,
           uploadPrivateKey,
           uploadCustomEnvVars,
           Stream.eval(uploadGpuDockerCompose)
    ).parJoin(
      6
    )
  }

  private def getBucketSAs(serviceAccountInfo: WorkbenchEmail): Stream[F, List[Identity]] =
    Stream.eval(Async[F].pure(List(serviceAccountIdentity(serviceAccountInfo))))

  private def serviceAccountIdentity(email: WorkbenchEmail) = Identity.serviceAccount(email.value)
  private def userIdentity(email: WorkbenchEmail) = Identity.user(email.value)
  private def groupIdentity(email: WorkbenchEmail) = Identity.group(email.value)
}

case class BucketHelperConfig(imageConfig: ImageConfig,
                              welderConfig: WelderConfig,
                              proxyConfig: ProxyConfig,
                              clusterFilesConfig: SecurityFilesConfig
)
