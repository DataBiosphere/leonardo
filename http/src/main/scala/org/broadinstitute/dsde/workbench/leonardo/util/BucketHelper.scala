package org.broadinstitute.dsde.workbench.leonardo
package util

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import _root_.org.typelevel.log4cats.Logger
import cats.data.NonEmptyList
import cats.effect.{Async, Blocker, Concurrent, ContextShift}
import cats.syntax.all._
import cats.mtl.Ask
import com.google.cloud.Identity
import fs2._
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService, StorageRole}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, ServiceAccountKey}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}

class BucketHelper[F[_]: Concurrent: ContextShift: Logger](config: BucketHelperConfig,
                                                           google2StorageDAO: GoogleStorageService[F],
                                                           serviceAccountProvider: ServiceAccountProvider[F],
                                                           blocker: Blocker) {

  val leoEntity = serviceAccountIdentity(Config.serviceAccountProviderConfig.leoServiceAccountEmail)

  /**
   * Creates the dataproc init bucket and sets the necessary ACLs.
   */
  def createInitBucket(googleProject: GoogleProject, bucketName: GcsBucketName, serviceAccount: WorkbenchEmail)(
    implicit ev: Ask[F, TraceId]
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

      _ <- google2StorageDAO.insertBucket(googleProject, bucketName, traceId = Some(ctx))
      _ <- google2StorageDAO.setIamPolicy(bucketName, (readerAcl ++ ownerAcl).toMap, traceId = Some(ctx))
    } yield ()

  /**
   * Creates the dataproc staging bucket and sets the necessary ACLs.
   */
  def createStagingBucket(
    userEmail: WorkbenchEmail,
    googleProject: GoogleProject,
    bucketName: GcsBucketName,
    serviceAccountInfo: WorkbenchEmail
  )(implicit ev: Ask[F, TraceId]): Stream[F, Unit] =
    for {
      ctx <- Stream.eval(ev.ask)
      // The staging bucket is created in the cluster's project.
      // Leo service account -> Owner
      // Available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Owner
      // Additional readers (users and groups) are specified by the service account provider.
      bucketSAs <- getBucketSAs(serviceAccountInfo)
      providerReaders <- Stream.eval(
        serviceAccountProvider.listUsersStagingBucketReaders(userEmail).map(_.map(userIdentity))
      )
      providerGroups <- Stream.eval(
        serviceAccountProvider.listGroupsStagingBucketReaders(userEmail).map(_.map(groupIdentity))
      )

      readerAcl = NonEmptyList
        .fromList(providerReaders ++ providerGroups)
        .map(readers => Map(StorageRole.ObjectViewer -> readers))
        .getOrElse(Map.empty)
      ownerAcl = Map(StorageRole.ObjectAdmin -> NonEmptyList(leoEntity, bucketSAs))

      _ <- google2StorageDAO.insertBucket(googleProject, bucketName, traceId = Some(ctx))
      _ <- google2StorageDAO.setIamPolicy(bucketName, (readerAcl ++ ownerAcl).toMap, traceId = Some(ctx))
    } yield ()

  def deleteInitBucket(googleProject: GoogleProject,
                       initBucketName: GcsBucketName)(implicit ev: Ask[F, TraceId]): F[Unit] =
    for {
      traceId <- ev.ask
      _ <- google2StorageDAO
        .deleteBucket(googleProject, initBucketName, isRecursive = true, traceId = Some(traceId))
        .compile
        .drain
    } yield ()

  def initializeBucketObjects(
    initBucketName: GcsBucketName,
    serviceAccountKey: Option[ServiceAccountKey],
    templateValues: RuntimeTemplateValues,
    customClusterEnvironmentVariables: Map[String, String]
  ): Stream[F, Unit] = {
    // Build a mapping of (name, value) pairs with which to apply templating logic to resources
    val replacements = templateValues.toMap

    // Jupyter allows setting of arbitrary environment variables on cluster creation if they are passed in to
    // docker-compose as a file of format:
    //     var1=value1
    //     var2=value2
    // etc. We're building a string of that format here.
    val customEnvVars = customClusterEnvironmentVariables.foldLeft("")({
      case (memo, (key, value)) => memo + s"$key=$value\n"
    })

    // Check if rstudioLicenseFile exists to allow the Leonardo PR to merge before the
    // actual license file is added to firecloud-develop.
    // TODO: remove once firecloud-develop has been updated to render the license file
    val rstudioLicenseFile = Stream
      .eval(
        Async[F].delay(Files.exists(config.clusterFilesConfig.rstudioLicenseFile)) map {
          case true  => Some(config.clusterFilesConfig.rstudioLicenseFile)
          case false => None
        }
      )
      .unNone

    val uploadRawFiles = for {
      f <- Stream.emits(
        Seq(
          config.clusterFilesConfig.proxyServerCrt,
          config.clusterFilesConfig.proxyServerKey,
          config.clusterFilesConfig.proxyRootCaPem
        )
      ) ++ rstudioLicenseFile
      _ <- TemplateHelper.fileStream[F](f, blocker) through google2StorageDAO.streamUploadBlob(
        initBucketName,
        GcsBlobName(f.getFileName.toString)
      )
    } yield ()

    val uploadRawResources = for {
      r <- Stream.emits(
        Seq(
          config.clusterResourcesConfig.jupyterDockerCompose,
          config.clusterResourcesConfig.jupyterDockerComposeGce,
          config.clusterResourcesConfig.rstudioDockerCompose,
          config.clusterResourcesConfig.proxyDockerCompose,
          config.clusterResourcesConfig.proxySiteConf,
          config.clusterResourcesConfig.welderDockerCompose,
          config.clusterResourcesConfig.cryptoDetectorDockerCompose,
          // Note: jupyter_notebook_config.py is non-templated and gets copied inside the Jupyter container.
          // So technically we could just put it in the Jupyter base image itself. However we would still need
          // it here to support legacy images where it is not present in the container.
          config.clusterResourcesConfig.jupyterNotebookConfigUri
        )
      )
      _ <- TemplateHelper.resourceStream[F](r, blocker) through google2StorageDAO.streamUploadBlob(
        initBucketName,
        GcsBlobName(r.asString)
      )
    } yield ()

    val uploadTemplatedResources = for {
      r <- Stream.emits(
        Seq(
          config.clusterResourcesConfig.initActionsScript,
          config.clusterResourcesConfig.gceInitScript,
          config.clusterResourcesConfig.jupyterNotebookFrontendConfigUri
        )
      )
      _ <- TemplateHelper.templateResource[F](replacements, r, blocker) through google2StorageDAO.streamUploadBlob(
        initBucketName,
        GcsBlobName(r.asString)
      )
    } yield ()

    val uploadPrivateKey = for {
      k <- Stream(serviceAccountKey).unNone
      data <- Stream(k.privateKeyData.decode).unNone
      _ <- Stream.emits(data.getBytes(StandardCharsets.UTF_8)) through google2StorageDAO.streamUploadBlob(
        initBucketName,
        GcsBlobName(RuntimeTemplateValues.serviceAccountCredentialsFilename)
      )
    } yield ()

    val uploadCustomEnvVars = Stream.emits(customEnvVars.getBytes(StandardCharsets.UTF_8)) through google2StorageDAO
      .streamUploadBlob(initBucketName, GcsBlobName(config.clusterResourcesConfig.customEnvVarsConfigUri.asString))

    Stream(uploadRawFiles, uploadRawResources, uploadTemplatedResources, uploadPrivateKey, uploadCustomEnvVars).parJoin(
      5
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
                              clusterFilesConfig: SecurityFilesConfig,
                              clusterResourcesConfig: ClusterResourcesConfig)
