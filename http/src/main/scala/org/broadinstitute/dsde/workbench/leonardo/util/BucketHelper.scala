package org.broadinstitute.dsde.workbench.leonardo
package util

import java.nio.charset.StandardCharsets

import _root_.io.chrisdavenport.log4cats.Logger
import cats.data.{NonEmptyList, OptionT}
import cats.effect.{Async, Blocker, Concurrent, ContextShift, IO}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.Identity
import fs2._
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleComputeService, GoogleStorageService, StorageRole}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, ServiceAccountKey}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}

class BucketHelper[F[_]: Concurrent: ContextShift: Logger](config: BucketHelperConfig,
                                                           googleComputeService: GoogleComputeService[F],
                                                           google2StorageDAO: GoogleStorageService[F],
                                                           googleProjectDAO: GoogleProjectDAO,
                                                           serviceAccountProvider: ServiceAccountProvider[F],
                                                           blocker: Blocker)(implicit cs: ContextShift[IO]) {

  val leoEntity = serviceAccountIdentity(Config.serviceAccountProviderConfig.leoServiceAccountEmail)

  /**
   * Creates the dataproc init bucket and sets the necessary ACLs.
   */
  def createInitBucket(googleProject: GoogleProject, bucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): Stream[F, Unit] =
    for {
      ctx <- Stream.eval(ev.ask)
      // The init bucket is created in the cluster's project.
      // Leo service account -> Owner
      // available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Reader
      bucketSAs <- getBucketSAs(googleProject, serviceAccountInfo)

      readerAcl = NonEmptyList
        .fromList(bucketSAs)
        .map(readers => Map(StorageRole.ObjectViewer -> readers))
        .getOrElse(Map.empty)
      ownerAcl = Map(StorageRole.ObjectAdmin -> NonEmptyList.one(leoEntity))

      _ <- google2StorageDAO.insertBucket(googleProject, bucketName, traceId = Some(ctx))
      _ <- google2StorageDAO.setIamPolicy(bucketName, readerAcl ++ ownerAcl, traceId = Some(ctx))
    } yield ()

  /**
   * Creates the dataproc staging bucket and sets the necessary ACLs.
   */
  def createStagingBucket(
    userEmail: WorkbenchEmail,
    googleProject: GoogleProject,
    bucketName: GcsBucketName,
    serviceAccountInfo: ServiceAccountInfo
  )(implicit ev: ApplicativeAsk[F, TraceId]): Stream[F, Unit] =
    for {
      ctx <- Stream.eval(ev.ask)
      // The staging bucket is created in the cluster's project.
      // Leo service account -> Owner
      // Available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Owner
      // Additional readers (users and groups) are specified by the service account provider.
      bucketSAs <- getBucketSAs(googleProject, serviceAccountInfo)
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
      _ <- google2StorageDAO.setIamPolicy(bucketName, readerAcl ++ ownerAcl, traceId = Some(ctx))
    } yield ()

  def storeObject(bucketName: GcsBucketName, objectName: GcsBlobName, objectContents: Array[Byte], objectType: String)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): Stream[F, Unit] =
    for {
      traceId <- Stream.eval(ev.ask)
      _ <- google2StorageDAO
        .createBlob(bucketName, objectName, objectContents, objectType, traceId = Some(traceId))
        .void
    } yield ()

  def deleteInitBucket(googleProject: GoogleProject,
                       initBucketName: GcsBucketName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
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
  )(implicit ev: ApplicativeAsk[F, TraceId]): Stream[F, Unit] = {
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

    val uploadRawFiles = for {
      f <- Stream.emits(
        Seq(
          config.clusterFilesConfig.jupyterServerCrt,
          config.clusterFilesConfig.jupyterServerKey,
          config.clusterFilesConfig.jupyterRootCaPem
        )
      )
      bytes <- Stream.eval(TemplateHelper.fileStream[F](f, blocker).compile.to(Array))
      _ <- storeObject(initBucketName, GcsBlobName(f.getName), bytes, "text/plain")
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
          // Note: jupyter_notebook_config.py is non-templated and gets copied inside the Jupyter container.
          // So technically we could just put it in the Jupyter base image itself. However we would still need
          // it here to support legacy images where it is not present in the container.
          config.clusterResourcesConfig.jupyterNotebookConfigUri
        )
      )
      bytes <- Stream.eval(TemplateHelper.resourceStream[F](r, blocker).compile.to(Array))
      _ <- storeObject(initBucketName, GcsBlobName(r.asString), bytes, "text/plain")
    } yield ()

    val uploadTemplatedResources = for {
      r <- Stream.emits(
        Seq(
          config.clusterResourcesConfig.initActionsScript,
          config.clusterResourcesConfig.gceInitScript,
          config.clusterResourcesConfig.jupyterNotebookFrontendConfigUri
        )
      )
      bytes <- Stream.eval(TemplateHelper.templateResource[F](replacements, r, blocker).compile.to(Array))
      _ <- storeObject(initBucketName, GcsBlobName(r.asString), bytes, "text/plain")
    } yield ()

    val uploadPrivateKey = for {
      k <- Stream(serviceAccountKey).unNone
      data <- Stream(k.privateKeyData.decode).unNone
      _ <- storeObject(initBucketName,
                       GcsBlobName(RuntimeTemplateValues.serviceAccountCredentialsFilename),
                       data.getBytes(StandardCharsets.UTF_8),
                       "text/plain")
    } yield ()

    val uploadCustomEnvVars = storeObject(initBucketName,
                                          GcsBlobName(config.clusterResourcesConfig.customEnvVarsConfigUri.asString),
                                          customEnvVars.getBytes(StandardCharsets.UTF_8),
                                          "text/plain")

    Stream(uploadRawFiles, uploadRawResources, uploadTemplatedResources, uploadPrivateKey, uploadCustomEnvVars).parJoin(
      5
    )
  }

  private def getBucketSAs(googleProject: GoogleProject,
                           serviceAccountInfo: ServiceAccountInfo): Stream[F, List[Identity]] = {
    val computeDefaultSA = for {
      projectNumber <- OptionT(
        Async[F].liftIO(IO.fromFuture(IO(googleProjectDAO.getProjectNumber(googleProject.value))))
      )
      sa <- OptionT.pure[F](googleComputeService.getComputeEngineDefaultServiceAccount(projectNumber))
    } yield sa

    // cluster SA orElse compute engine default SA
    val clusterOrComputeDefault =
      OptionT.fromOption[F](serviceAccountInfo.clusterServiceAccount) orElse computeDefaultSA

    // List(cluster or default SA, notebook SA) if they exist
    val identities = clusterOrComputeDefault.value.map { clusterOrDefaultSAOpt =>
      List(clusterOrDefaultSAOpt, serviceAccountInfo.notebookServiceAccount).flatten.map(serviceAccountIdentity)
    }

    Stream.eval(identities)
  }

  private def serviceAccountIdentity(email: WorkbenchEmail) = Identity.serviceAccount(email.value)
  private def userIdentity(email: WorkbenchEmail) = Identity.user(email.value)
  private def groupIdentity(email: WorkbenchEmail) = Identity.group(email.value)
}

case class BucketHelperConfig(imageConfig: ImageConfig,
                              welderConfig: WelderConfig,
                              proxyConfig: ProxyConfig,
                              clusterFilesConfig: ClusterFilesConfig,
                              clusterResourcesConfig: ClusterResourcesConfig)
