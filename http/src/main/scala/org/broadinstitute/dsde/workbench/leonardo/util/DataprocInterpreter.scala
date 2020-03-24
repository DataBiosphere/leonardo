package org.broadinstitute.dsde.workbench.leonardo
package util

import _root_.io.chrisdavenport.log4cats.Logger
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.data.OptionT
import cats.effect.{Async, _}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.admin.directory.model.Group
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.google._
import org.broadinstitute.dsde.workbench.google2.{DiskName, GoogleComputeService, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CustomImage.DataprocCustomImage
import org.broadinstitute.dsde.workbench.leonardo.DataprocRole.Master
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.service.InvalidDataprocMachineConfigException
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.util.RuntimeInterpreterConfig.DataprocInterpreterConfig
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import org.broadinstitute.dsde.workbench.util.Retry
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

final case class ClusterIamSetupException(googleProject: GoogleProject)
    extends LeoException(s"Error occurred setting up IAM roles in project ${googleProject.value}")

final case class GoogleGroupCreationException(googleGroup: WorkbenchEmail, msg: String)
    extends LeoException(s"Failed to create the Google group '${googleGroup}': $msg", StatusCodes.InternalServerError)

final case object ImageProjectNotFoundException
    extends LeoException("Custom Dataproc image project not found", StatusCodes.NotFound)

final case class ClusterResourceConstaintsException(clusterProjectAndName: RuntimeProjectAndName,
                                                    machineType: MachineTypeName)
    extends LeoException(
      s"Unable to calculate memory constraints for cluster ${clusterProjectAndName.googleProject}/${clusterProjectAndName.runtimeName} with master machine type ${machineType}"
    )

class DataprocInterpreter[F[_]: Async: Parallel: ContextShift: Logger](
  config: DataprocInterpreterConfig,
  bucketHelper: BucketHelper[F],
  vpcAlg: VPCAlgebra[F],
  gdDAO: GoogleDataprocDAO,
  googleComputeService: GoogleComputeService[F],
  googleDirectoryDAO: GoogleDirectoryDAO,
  googleIamDAO: GoogleIamDAO,
  googleProjectDAO: GoogleProjectDAO,
  welderDao: WelderDAO[F],
  blocker: Blocker
)(implicit val executionContext: ExecutionContext,
  val system: ActorSystem,
  contextShift: ContextShift[IO], // needed for IO.fromFuture(...)
  metrics: NewRelicMetrics[F],
  dbRef: DbReference[F])
    extends BaseRuntimeInterpreter[F](config, welderDao)
    with RuntimeAlgebra[F]
    with LazyLogging
    with Retry {

  import dbRef._

  override def createRuntime(
    params: CreateRuntimeParams
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[CreateRuntimeResponse] = {
    val initBucketName = generateUniqueBucketName("leoinit-" + params.runtimeProjectAndName.runtimeName.asString)
    val stagingBucketName = generateUniqueBucketName("leostaging-" + params.runtimeProjectAndName.runtimeName.asString)

    // Generate a service account key for the notebook service account (if present) to localize on the cluster.
    // We don't need to do this for the cluster service account because its credentials are already
    // on the metadata server.
    generateServiceAccountKey(params.runtimeProjectAndName.googleProject,
                              params.serviceAccountInfo.notebookServiceAccount).flatMap { serviceAccountKeyOpt =>
      val ioResult = for {
        // Set up VPC network and firewall
        (network, subnetwork) <- vpcAlg.setUpProjectNetwork(
          SetUpProjectNetworkParams(params.runtimeProjectAndName.googleProject)
        )
        _ <- vpcAlg.setUpProjectFirewalls(
          SetUpProjectFirewallsParams(params.runtimeProjectAndName.googleProject, network)
        )

        resourceConstraints <- getClusterResourceContraints(params.runtimeProjectAndName,
                                                            params.runtimeConfig.machineType)

        // Set up IAM roles necessary to create a cluster.
        _ <- createClusterIamRoles(params.runtimeProjectAndName.googleProject, params.serviceAccountInfo)

        // Add member to the Google Group that has the IAM role to pull the Dataproc image
        _ <- updateDataprocImageGroupMembership(params.runtimeProjectAndName.googleProject, createCluster = true)

        // Create the bucket in the cluster's google project and populate with initialization files.
        // ACLs are granted so the cluster service account can access the files at initialization time.
        _ <- bucketHelper
          .createInitBucket(params.runtimeProjectAndName.googleProject, initBucketName, params.serviceAccountInfo)
          .compile
          .drain

        // Create the cluster staging bucket. ACLs are granted so the user/pet can access it.
        _ <- bucketHelper
          .createStagingBucket(params.auditInfo.creator,
                               params.runtimeProjectAndName.googleProject,
                               stagingBucketName,
                               params.serviceAccountInfo)
          .compile
          .drain

        templateParams = RuntimeTemplateValuesConfig.fromCreateRuntimeParams(
          params,
          Some(initBucketName),
          Some(stagingBucketName),
          serviceAccountKeyOpt,
          config.imageConfig,
          config.welderConfig,
          config.proxyConfig,
          config.clusterFilesConfig,
          config.clusterResourcesConfig,
          Some(resourceConstraints)
        )
        _ <- bucketHelper
          .initializeBucketObjects(initBucketName, templateParams, params.customEnvironmentVariables)
          .compile
          .drain

        // build cluster configuration
        initScriptResources = List(config.clusterResourcesConfig.initActionsScript)
        initScripts = initScriptResources.map(resource => GcsPath(initBucketName, GcsObjectName(resource.asString)))
        credentialsFileName = params.serviceAccountInfo.notebookServiceAccount
          .map(_ => s"/etc/${RuntimeTemplateValues.serviceAccountCredentialsFilename}")

        // If user is using https://github.com/DataBiosphere/terra-docker/tree/master#terra-base-images for jupyter image, then
        // we will use the new custom dataproc image
        dataprocImage = if (params.runtimeImages.exists(_.imageUrl == config.imageConfig.legacyJupyterImage.imageUrl))
          config.dataprocConfig.legacyCustomDataprocImage
        else config.dataprocConfig.customDataprocImage

        res <- params.runtimeConfig match {
          case _: RuntimeConfig.GceConfig => Async[F].raiseError[CreateRuntimeResponse](new NotImplementedException)
          case x: RuntimeConfig.DataprocConfig =>
            val createClusterConfig = CreateClusterConfig(
              x,
              initScripts,
              params.serviceAccountInfo.clusterServiceAccount,
              credentialsFileName,
              stagingBucketName,
              params.scopes,
              subnetwork,
              dataprocImage,
              config.monitorConfig.monitorStatusTimeouts.getOrElse(RuntimeStatus.Creating, 1 hour)
            )
            for { // Create the cluster
              retryResult <- Async[F].liftIO(
                IO.fromFuture(
                  IO(
                    retryExponentially(whenGoogleZoneCapacityIssue,
                                       "Cluster creation failed because zone with adequate resources was not found") {
                      () =>
                        gdDAO.createCluster(params.runtimeProjectAndName.googleProject,
                                            params.runtimeProjectAndName.runtimeName,
                                            createClusterConfig)
                    }
                  )
                )
              )
              operation <- retryResult match {
                case Right((errors, op)) if errors == List.empty => Async[F].pure(op)
                case Right((errors, op)) =>
                  metrics
                    .incrementCounter("zoneCapacityClusterCreationFailure", errors.length)
                    .as(op)
                case Left(errors) =>
                  metrics
                    .incrementCounter("zoneCapacityClusterCreationFailure",
                                      errors.filter(whenGoogleZoneCapacityIssue).length)
                    .flatMap(_ => Async[F].raiseError[Operation](errors.head))
              }

              asyncRuntimeFields = AsyncRuntimeFields(operation.id, operation.name, stagingBucketName, None)
            } yield CreateRuntimeResponse(asyncRuntimeFields, initBucketName, serviceAccountKeyOpt, dataprocImage)
        }
      } yield res

      ioResult.handleErrorWith { throwable =>
        cleanUpGoogleResourcesOnError(params.runtimeProjectAndName.googleProject,
                                      params.runtimeProjectAndName.runtimeName,
                                      initBucketName,
                                      params.serviceAccountInfo,
                                      serviceAccountKeyOpt) >> Async[F].raiseError(throwable)
      }
    }
  }

  override def getRuntimeStatus(
    params: GetRuntimeStatusParams
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[RuntimeStatus] =
    Async[F].liftIO(IO.fromFuture(IO(gdDAO.getClusterStatus(params.googleProject, params.runtimeName)))).map {
      clusterStatusOpt =>
        clusterStatusOpt.fold[RuntimeStatus](RuntimeStatus.Deleted)(RuntimeStatus.fromDataprocClusterStatus)
    }

  override def deleteRuntime(params: DeleteRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      // Delete the notebook service account key in Google, if present
      keyIdOpt <- dbRef.inTransaction {
        clusterQuery.getServiceAccountKeyId(params.runtime.googleProject, params.runtime.runtimeName)
      }
      _ <- removeServiceAccountKey(params.runtime.googleProject,
                                   params.runtime.serviceAccountInfo.notebookServiceAccount,
                                   keyIdOpt)
        .recoverWith {
          case NonFatal(e) =>
            Logger[F].error(e)(
              s"Error occurred removing service account key for ${params.runtime.googleProject} / ${params.runtime.runtimeName}"
            )
        }
      hasDataprocInfo = params.runtime.asyncRuntimeFields.isDefined
      _ <- if (hasDataprocInfo)
        Async[F].liftIO(
          IO.fromFuture(IO(gdDAO.deleteCluster(params.runtime.googleProject, params.runtime.runtimeName)))
        )
      else Async[F].unit
    } yield ()

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      _ <- removeClusterIamRoles(params.runtime.googleProject, params.runtime.serviceAccountInfo)
      _ <- updateDataprocImageGroupMembership(params.runtime.googleProject, createCluster = false)
    } yield ()

  override protected def stopGoogleRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit] =
    for {
      metadata <- getShutdownScript(runtime, blocker)

      // First remove all its preemptible instances, if any
      _ <- runtimeConfig match {
        case x: RuntimeConfig.DataprocConfig if x.numberOfPreemptibleWorkers.exists(_ > 0) =>
          Async[F].liftIO(
            IO.fromFuture(
              IO(gdDAO.resizeCluster(runtime.googleProject, runtime.runtimeName, numPreemptibles = Some(0)))
            )
          )
        case _ => Async[F].unit
      }

      // Now stop each instance individually
      _ <- runtime.nonPreemptibleInstances.toList.parTraverse { instance =>
        instance.dataprocRole match {
          case Master =>
            googleComputeService.addInstanceMetadata(
              instance.key.project,
              instance.key.zone,
              instance.key.name,
              metadata
            ) >> googleComputeService.stopInstance(instance.key.project, instance.key.zone, instance.key.name)
          case _ =>
            googleComputeService.stopInstance(instance.key.project, instance.key.zone, instance.key.name)
        }
      }
    } yield ()

  override protected def startGoogleRuntime(runtime: Runtime,
                                            welderAction: Option[WelderAction],
                                            runtimeConfig: RuntimeConfig)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit] =
    for {
      metadata <- getStartupScript(runtime, welderAction, blocker)

      // Add back the preemptible instances, if any
      _ <- runtimeConfig match {
        case x: RuntimeConfig.DataprocConfig if (x.numberOfPreemptibleWorkers.exists(_ > 0)) =>
          Async[F].liftIO(
            IO.fromFuture(
              IO(
                gdDAO.resizeCluster(runtime.googleProject,
                                    runtime.runtimeName,
                                    numPreemptibles = x.numberOfPreemptibleWorkers)
              )
            )
          )
        case _ => Async[F].unit
      }

      // Start each instance individually
      _ <- runtime.nonPreemptibleInstances.toList.parTraverse { instance =>
        // Install a startup script on the master node so Jupyter starts back up again once the instance is restarted
        instance.dataprocRole match {
          case Master =>
            googleComputeService.addInstanceMetadata(
              instance.key.project,
              instance.key.zone,
              instance.key.name,
              metadata
            ) >> googleComputeService.startInstance(instance.key.project, instance.key.zone, instance.key.name)
          case _ =>
            googleComputeService.startInstance(instance.key.project, instance.key.zone, instance.key.name)
        }
      }

    } yield ()

  override def resizeCluster(params: ResizeClusterParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    (for {
      // IAM roles should already exist for a non-deleted cluster; this method is a no-op if the roles already exist.
      _ <- createClusterIamRoles(params.runtime.googleProject, params.runtime.serviceAccountInfo)

      _ <- updateDataprocImageGroupMembership(params.runtime.googleProject, createCluster = true)

      // Resize the cluster in Google
      _ <- Async[F].liftIO(
        IO.fromFuture(
          IO(
            gdDAO.resizeCluster(params.runtime.googleProject,
                                params.runtime.runtimeName,
                                params.numWorkers,
                                params.numPreemptibles)
          )
        )
      )
    } yield ()) recoverWith {
      case gjre: GoogleJsonResponseException =>
        // Typically we will revoke this role in the monitor after everything is complete, but if Google fails to
        // resize the cluster we need to revoke it manually here
        for {
          _ <- removeClusterIamRoles(params.runtime.googleProject, params.runtime.serviceAccountInfo)
          // Remove member from the Google Group that has the IAM role to pull the Dataproc image
          _ <- updateDataprocImageGroupMembership(params.runtime.googleProject, createCluster = false)
          _ <- Logger[F].error(gjre)(s"Could not successfully update cluster ${params.runtime.projectNameString}")
          _ <- Async[F].raiseError[Unit](InvalidDataprocMachineConfigException(gjre.getMessage))
        } yield ()
    }

  //updates machine type in gdDAO
  override protected def setMachineTypeInGoogle(runtime: Runtime, machineType: MachineTypeName)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit] =
    runtime.dataprocInstances.toList.traverse_ { instance =>
      // Note: we don't support changing the machine type for worker instances. While this is possible
      // in GCP, Spark settings are auto-tuned to machine size. Dataproc recommends adding or removing nodes,
      // and rebuilding the cluster if new worker machine/disk sizes are needed.
      instance.dataprocRole match {
        case Master =>
          googleComputeService.setMachineType(instance.key.project, instance.key.zone, instance.key.name, machineType)
        case _ => Async[F].unit
      }
    }

  override def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    params.runtime.dataprocInstances.toList.traverse_ { instance =>
      // Note: we don't support changing the machine type for worker instances. While this is possible
      // in GCP, Spark settings are auto-tuned to machine size. Dataproc recommends adding or removing nodes,
      // and rebuilding the cluster if new worker machine/disk sizes are needed.
      instance.dataprocRole match {
        case Master =>
          // Note for Dataproc the disk name is the same as the instance name
          googleComputeService.resizeDisk(instance.key.project,
                                          instance.key.zone,
                                          DiskName(instance.key.name.value),
                                          params.diskSize)
        case _ => Async[F].unit
      }
    }

  def createClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): F[Unit] =
    updateClusterIamRoles(googleProject, serviceAccountInfo, createCluster = true)

  def removeClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): F[Unit] =
    updateClusterIamRoles(googleProject, serviceAccountInfo, createCluster = false)

  def generateServiceAccountKey(googleProject: GoogleProject,
                                serviceAccountEmailOpt: Option[WorkbenchEmail]): F[Option[ServiceAccountKey]] =
    serviceAccountEmailOpt.traverse { email =>
      Async[F].liftIO(IO.fromFuture(IO(googleIamDAO.createServiceAccountKey(googleProject, email))))
    }

  def removeServiceAccountKey(googleProject: GoogleProject,
                              serviceAccountEmailOpt: Option[WorkbenchEmail],
                              serviceAccountKeyIdOpt: Option[ServiceAccountKeyId]): F[Unit] =
    (serviceAccountEmailOpt, serviceAccountKeyIdOpt).mapN {
      case (email, keyId) =>
        Async[F].liftIO(IO.fromFuture(IO(googleIamDAO.removeServiceAccountKey(googleProject, email, keyId))))
    } getOrElse Async[F].unit

  def setupDataprocImageGoogleGroup(): F[Unit] =
    createDataprocImageUserGoogleGroupIfItDoesntExist() >>
      addIamRoleToDataprocImageGroup(config.dataprocConfig.customDataprocImage)

  /**
   * Add the user's service account to the Google group.
   * This group has compute.imageUser role on the custom Dataproc image project,
   * which allows the user's cluster to pull the image.
   */
  def updateDataprocImageGroupMembership(googleProject: GoogleProject, createCluster: Boolean): F[Unit] =
    parseImageProject(config.dataprocConfig.customDataprocImage).traverse_ { imageProject =>
      for {
        count <- inTransaction { clusterQuery.countActiveByProject(googleProject) }
        // Note: Don't remove the account if there are existing active clusters in the same project,
        // because it could potentially break other clusters. We only check this for the 'remove' case.
        _ <- if (count > 0 && !createCluster) {
          Async[F].unit
        } else {
          for {
            projectNumberOptIO <- Async[F].liftIO(
              IO.fromFuture(IO(googleProjectDAO.getProjectNumber(googleProject.value)))
            )
            projectNumber <- Async[F].liftIO(
              IO.fromEither(projectNumberOptIO.toRight(ClusterIamSetupException(imageProject)))
            )
            // Note that the Dataproc service account is used to retrieve the image, and not the user's
            // pet service account. There is one Dataproc service account per Google project. For more details:
            // https://cloud.google.com/dataproc/docs/concepts/iam/iam#service_accounts
            dataprocServiceAccountEmail = WorkbenchEmail(
              s"service-${projectNumber}@dataproc-accounts.iam.gserviceaccount.com"
            )
            _ <- updateGroupMembership(config.groupsConfig.dataprocImageProjectGroupEmail,
                                       dataprocServiceAccountEmail,
                                       createCluster)
          } yield ()
        }
      } yield ()
    }

  private def cleanUpGoogleResourcesOnError(googleProject: GoogleProject,
                                            clusterName: RuntimeName,
                                            initBucketName: GcsBucketName,
                                            serviceAccountInfo: ServiceAccountInfo,
                                            serviceAccountKeyOpt: Option[ServiceAccountKey]): F[Unit] = {
    // Clean up resources in Google
    val deleteBucket = bucketHelper.deleteInitBucket(initBucketName).attempt.flatMap {
      case Left(e) =>
        Logger[F].error(e)(
          s"Failed to delete init bucket ${initBucketName.value} for ${googleProject.value} / ${clusterName.asString}"
        )
      case _ =>
        Logger[F].info(
          s"Successfully deleted init bucket ${initBucketName.value} for ${googleProject.value} / ${clusterName.asString}"
        )
    }

    // Don't delete the staging bucket so the user can see error logs.

    val deleteCluster =
      Async[F].liftIO(IO.fromFuture(IO(gdDAO.deleteCluster(googleProject, clusterName)))).attempt.flatMap {
        case Left(e) => Logger[F].error(e)(s"Failed to delete cluster ${googleProject.value} / ${clusterName.asString}")
        case _       => Logger[F].info(s"Successfully deleted cluster ${googleProject.value} / ${clusterName.asString}")
      }

    val deleteServiceAccountKey = removeServiceAccountKey(googleProject,
                                                          serviceAccountInfo.notebookServiceAccount,
                                                          serviceAccountKeyOpt.map(_.id)).attempt.flatMap {
      case Left(e) =>
        Logger[F].error(e)(s"Failed to delete service account key for ${serviceAccountInfo.notebookServiceAccount}")
      case _ =>
        Logger[F].info(s"Successfully deleted service account key for ${serviceAccountInfo.notebookServiceAccount}")
    }

    val removeIamRoles = removeClusterIamRoles(googleProject, serviceAccountInfo).attempt.flatMap {
      case Left(e) =>
        Logger[F].error(e)(s"Failed to remove IAM roles for ${googleProject.value} / ${clusterName.asString}")
      case _ => Logger[F].info(s"Successfully removed IAM roles for ${googleProject.value} / ${clusterName.asString}")
    }

    List(deleteBucket, deleteCluster, deleteServiceAccountKey, removeIamRoles).parSequence_
  }

  private[leonardo] def getClusterResourceContraints(runtimeProjectAndName: RuntimeProjectAndName,
                                                     machineType: MachineTypeName)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[RuntimeResourceConstraints] = {
    val totalMemory = for {
      // Find a zone in which to query the machine type: either the configured zone or
      // an arbitrary zone in the configured region.
      zoneUri <- {
        val configuredZone = OptionT.fromOption[F](config.dataprocConfig.zoneName)
        val zoneList = for {
          zones <- googleComputeService.getZones(runtimeProjectAndName.googleProject, config.dataprocConfig.regionName)
          _ <- Logger[F].debug(s"List of zones in project ${runtimeProjectAndName.googleProject}: ${zones}")
          zoneNames = zones.map(z => ZoneName(z.getName))
        } yield zoneNames

        configuredZone orElse OptionT(zoneList.map(_.headOption))
      }
      _ <- OptionT.liftF(Logger[F].debug(s"Using zone ${zoneUri} to resolve machine type"))

      // Resolve the master machine type in Google to get the total memory.
      machineType <- OptionT.pure[F](machineType)
      resolvedMachineType <- OptionT(
        googleComputeService.getMachineType(runtimeProjectAndName.googleProject, zoneUri, machineType)
      )
      _ <- OptionT.liftF(Logger[F].debug(s"Resolved machine type: ${resolvedMachineType.toString}"))
    } yield MemorySize.fromMb(resolvedMachineType.getMemoryMb.toDouble)

    totalMemory.value.flatMap {
      case None        => Async[F].raiseError(ClusterResourceConstaintsException(runtimeProjectAndName, machineType))
      case Some(total) =>
        // total - dataproc allocated - welder allocated
        val dataprocAllocated = config.dataprocConfig.dataprocReservedMemory.map(_.bytes).getOrElse(0L)
        val welderAllocated = config.welderConfig.welderReservedMemory.map(_.bytes).getOrElse(0L)
        val result = MemorySize(total.bytes - dataprocAllocated - welderAllocated)
        Async[F].pure(RuntimeResourceConstraints(result))
    }
  }

  /**
   * Add the Dataproc Worker role in the user's project to the cluster service account, if present.
   * This is needed to be able to spin up Dataproc clusters using a custom service account.
   * If the Google Compute default service account is being used, this is not necessary.
   */
  private def updateClusterIamRoles(googleProject: GoogleProject,
                                    serviceAccountInfo: ServiceAccountInfo,
                                    createCluster: Boolean): F[Unit] = {
    val retryIam: (GoogleProject, WorkbenchEmail, Set[String]) => F[Unit] = (project, email, roles) =>
      Async[F].liftIO(
        IO.fromFuture[Unit](IO(retryExponentially(when409, s"IAM policy change failed for Google project '$project'") {
          () =>
            if (createCluster) {
              googleIamDAO.addIamRoles(project, email, MemberType.ServiceAccount, roles).void
            } else {
              googleIamDAO.removeIamRoles(project, email, MemberType.ServiceAccount, roles).void
            }
        }))
      )

    serviceAccountInfo.clusterServiceAccount.traverse_ { email =>
      // Note: don't remove the role if there are existing active clusters owned by the same user,
      // because it could potentially break other clusters. We only check this for the 'remove' case,
      // it's ok to re-add the roles.
      dbRef.inTransaction { clusterQuery.countActiveByClusterServiceAccount(email) }.flatMap { count =>
        if (count > 0 && !createCluster) {
          Async[F].unit
        } else {
          retryIam(googleProject, email, Set("roles/dataproc.worker"))
        }
      }
    }
  }

  private def createDataprocImageUserGoogleGroupIfItDoesntExist(): F[Unit] =
    for {
      _ <- Logger[F].debug(
        s"Checking if Dataproc image user Google group '${config.groupsConfig.dataprocImageProjectGroupEmail}' already exists..."
      )

      groupOpt <- Async[F].liftIO(
        IO.fromFuture[Option[Group]](
          IO(googleDirectoryDAO.getGoogleGroup(config.groupsConfig.dataprocImageProjectGroupEmail))
        )
      )
      _ <- groupOpt.fold(
        Logger[F].debug(
          s"Dataproc image user Google group '${config.groupsConfig.dataprocImageProjectGroupEmail}' does not exist. Attempting to create it..."
        ) >> createDataprocImageUserGoogleGroup()
      )(
        group =>
          Logger[F].debug(
            s"Dataproc image user Google group '${config.groupsConfig.dataprocImageProjectGroupEmail}' already exists: $group \n Won't attempt to create it."
          )
      )
    } yield ()

  private def createDataprocImageUserGoogleGroup(): F[Unit] =
    Async[F]
      .liftIO(
        IO.fromFuture(
          IO(
            googleDirectoryDAO
              .createGroup(
                config.groupsConfig.dataprocImageProjectGroupName,
                config.groupsConfig.dataprocImageProjectGroupEmail,
                Option(googleDirectoryDAO.lockedDownGroupSettings)
              )
          )
        )
      )
      .handleErrorWith {
        case t if when409(t) => Async[F].unit
        case t =>
          Async[F].raiseError(
            GoogleGroupCreationException(config.groupsConfig.dataprocImageProjectGroupEmail, t.getMessage)
          )
      }

  private def addIamRoleToDataprocImageGroup(customDataprocImage: DataprocCustomImage): F[Unit] = {
    val computeImageUserRole = Set("roles/compute.imageUser")
    parseImageProject(config.dataprocConfig.customDataprocImage).fold(
      Async[F].raiseError[Unit](ImageProjectNotFoundException)
    ) { imageProject =>
      for {
        _ <- Logger[F].debug(
          s"Attempting to grant 'compute.imageUser' permissions to '${config.groupsConfig.dataprocImageProjectGroupEmail}' on project '$imageProject' ..."
        )
        _ <- Async[F].liftIO(
          IO.fromFuture[Boolean](
            IO(
              retryExponentially(
                when409,
                s"IAM policy change failed for '${config.groupsConfig.dataprocImageProjectGroupEmail}' on Google project '$imageProject'."
              ) { () =>
                googleIamDAO.addIamRoles(imageProject,
                                         config.groupsConfig.dataprocImageProjectGroupEmail,
                                         MemberType.Group,
                                         computeImageUserRole)
              }
            )
          )
        )
      } yield ()
    }
  }

  private def updateGroupMembership(groupEmail: WorkbenchEmail,
                                    memberEmail: WorkbenchEmail,
                                    addToGroup: Boolean): F[Unit] =
    Async[F].liftIO(IO.fromFuture[Unit] {
      IO {
        retryExponentially(when409, s"Could not update group '$groupEmail' for member '$memberEmail'") {
          () =>
            logger.debug(s"Checking if '$memberEmail' is part of group '$groupEmail'...")
            googleDirectoryDAO.isGroupMember(groupEmail, memberEmail).flatMap {
              case false if (addToGroup) =>
                logger.debug(s"Adding '$memberEmail' to group '$groupEmail'...")
                googleDirectoryDAO.addMemberToGroup(groupEmail, memberEmail)
              case true if (!addToGroup) =>
                logger.debug(s"Removing '$memberEmail' from group '$groupEmail'...")
                googleDirectoryDAO.removeMemberFromGroup(groupEmail, memberEmail)
              case _ => Future.unit
            }
        }
      }
    })

  // See https://cloud.google.com/dataproc/docs/guides/dataproc-images#custom_image_uri
  private def parseImageProject(customDataprocImage: DataprocCustomImage): Option[GoogleProject] = {
    val regex = ".*projects/(.*)/global/images/(.*)".r
    customDataprocImage.asString match {
      case regex(project, _) => Some(GoogleProject(project))
      case _                 => None
    }
  }
}
