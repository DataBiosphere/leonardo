package org.broadinstitute.dsde.workbench.leonardo
package util

import _root_.org.typelevel.log4cats.StructuredLogger
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.cloud.compute.v1.{
  AccessConfig,
  Allowed,
  AttachedDisk,
  AttachedDiskInitializeParams,
  Disk,
  Firewall,
  Instance,
  Items,
  Metadata,
  NetworkInterface,
  ServiceAccount,
  Tags
}
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{
  streamFUntilDone,
  tracedRetryF,
  DiskName,
  FirewallRuleName,
  GoogleComputeService,
  NetworkName,
  RegionName,
  SubnetworkName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.config.GalaxyDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.AppDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{buildMachineTypeUri, buildSubnetworkUri}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.util.GalaxyVmInterpreter._
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountDisplayName}
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.cert.jcajce.{JcaX509CertificateConverter, JcaX509CertificateHolder, JcaX509v3CertificateBuilder}
import org.bouncycastle.openssl.{PEMKeyPair, PEMParser}
import org.bouncycastle.openssl.jcajce.{JcaPEMKeyConverter, JcaPEMWriter}
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder

import java.io.{ByteArrayInputStream, StringReader, StringWriter}
import java.math.BigInteger
import java.security.{KeyPairGenerator, SecureRandom}
import java.security.cert.{CertificateFactory, X509Certificate}
import java.time.{Duration, Instant}
import java.util.Date
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Provisions and deletes the GCE VM that runs a Galaxy app. */
class GalaxyVmInterpreter[F[_]](
  config: GKEInterpreterConfig,
  vpcAlg: VPCAlgebra[F],
  googleIamDAO: GoogleIamDAO,
  computeService: GoogleComputeService[F],
  appDao: AppDAO[F]
)(implicit
  executionContext: ExecutionContext,
  logger: StructuredLogger[F],
  dbRef: DbReference[F],
  F: Async[F]
) {
  implicit private def optionDoneCheckable[A]: DoneCheckable[Option[A]] = (a: Option[A]) => a.isDefined
  implicit private def booleanDoneCheckable: DoneCheckable[Boolean] = identity[Boolean]

  private[util] def installGalaxyVm(
    dbCluster: KubernetesCluster,
    app: App,
    nfsDisk: PersistentDisk,
    googleProject: GoogleProject,
    restore: Boolean
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      _ <- logger.info(ctx.loggingCtx)(
        s"Installing Galaxy VM for app ${app.appName.value} in project ${googleProject.value}"
      )

      zoneParam = nfsDisk.zone
      regionParam = RegionName(zoneParam.value.dropRight(2))

      // Set up VPC and firewall
      (network, subnetwork) <- vpcAlg.setUpProjectNetworkAndFirewalls(
        SetUpProjectNetworkParams(googleProject, regionParam)
      )

      // Load the startup-script passed to the GCE Guest Agent via the "startup-script" metadata key.
      // The Guest Agent executes it on every boot, unlike cloud-init user-data which the pre-baked
      // galaxy-k8s-boot image treats as already-run and skips.
      // To update, sync manually from https://github.com/galaxyproject/galaxy-k8s-boot/blob/anvil/bin/user_data.sh
      startupScriptContent <- F.fromTry(
        scala.util.Using(scala.io.Source.fromResource("init-resources/galaxy-user-data.sh"))(_.mkString)
      )

      // Derive postgres disk name using the same naming convention as the subscriber
      postgresDiskName = getGalaxyPostgresDiskName(nfsDisk.name, config.galaxyDiskConfig.postgresDiskNameSuffix)

      // Persistent-volume-size passed to ansible-pull.
      // nfsDisk.size.gb is in decimal GB; convert to binary GiB before subtracting filesystem overhead.
      // Example: a 500 GB disk = (500 * 10^9) / 2^30 ≈ 465 GiB, so we request 465 - 11 = 454 GiB.
      // Using raw gb - 11 (treating GB as GiB) overestimates by ~23 GiB on a 500 GB disk and
      // causes the NFS provisioner to fail with "insufficient available space".
      diskSizeGiB = (nfsDisk.size.gb.toLong * 1000L * 1000L * 1000L) / (1024L * 1024L * 1024L)
      pvSizeGi = math.max(1, diskSizeGiB - 11)
      pvSize = s"${pvSizeGi}Gi"

      // Get or create the galaxy-batch-runner SA in the user's project.
      gcpBatchSa <- F
        .fromFuture(
          F.delay(
            googleIamDAO.getOrCreateServiceAccount(
              googleProject,
              org.broadinstitute.dsde.workbench.model.google.ServiceAccountName("galaxy-batch-runner"),
              ServiceAccountDisplayName("Galaxy Batch Runner")
            )
          )
        )
        .map(sa => sa.email.value)

      galaxyUrlPrefix = s"/proxy/google/v1/apps/${googleProject.value}/${app.appName.value}/galaxy"

      // Disks — data and postgres disks are always pre-existing by the time this method runs
      // (created by createDiskOp / createSecondDiskOp, or retained from a previous app).
      // Use setSource to attach existing disks; only the boot disk is created fresh.
      bootDisk = AttachedDisk
        .newBuilder()
        .setBoot(true)
        .setAutoDelete(true)
        .setInitializeParams(
          AttachedDiskInitializeParams
            .newBuilder()
            .setSourceImage(config.galaxyVmConfig.sourceImage.asString)
            .setDiskSizeGb(config.galaxyVmConfig.bootDiskSizeGb.gb)
            .putAllLabels(Map("leonardo" -> "true").asJava)
            .build()
        )
        .build()

      // Galaxy data disk — device name must match what the bootstrap script expects
      dataDisk = AttachedDisk
        .newBuilder()
        .setBoot(false)
        .setDeviceName("galaxy-data")
        .setAutoDelete(false)
        .setSource(
          s"projects/${googleProject.value}/zones/${zoneParam.value}/disks/${nfsDisk.name.value}"
        )
        .build()

      // PostgreSQL disk — device name must match what the bootstrap script expects
      postgresDisk = AttachedDisk
        .newBuilder()
        .setBoot(false)
        .setDeviceName("galaxy-postgres-data")
        .setAutoDelete(false)
        .setSource(
          s"projects/${googleProject.value}/zones/${zoneParam.value}/disks/${postgresDiskName.value}"
        )
        .build()

      // Network interface with external IP
      networkInterface = NetworkInterface
        .newBuilder()
        .setSubnetwork(
          buildSubnetworkUri(googleProject, regionParam, subnetwork)
        )
        .addAccessConfigs(AccessConfig.newBuilder().setName("Leonardo Galaxy VM external IP").build())
        .build()

      instanceName = InstanceName(s"galaxy-${app.appName.value}")

      // Prefer the machine type stored on the app's nodepool (set from the request at creation
      // time); fall back to the configured default. Extracted here so it is available in
      // error messages if provisioning fails (e.g. ZONE_RESOURCE_POOL_EXHAUSTED).
      vmMachineType = dbCluster.nodepools
        .find(_.id == app.nodepoolId)
        .map(_.machineType)
        .getOrElse(config.galaxyVmConfig.machineType)

      instance = Instance
        .newBuilder()
        .setName(instanceName.value)
        .setDescription("Leonardo Galaxy VM")
        .setTags(Tags.newBuilder().addItems(config.vpcNetworkTag.value).build())
        .setMachineType(buildMachineTypeUri(zoneParam, vmMachineType))
        .addNetworkInterfaces(networkInterface)
        .addAllDisks(List(bootDisk, dataDisk, postgresDisk).asJava)
        .addServiceAccounts(
          ServiceAccount
            .newBuilder()
            .setEmail(app.googleServiceAccount.value)
            .addAllScopes(
              List(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/logging.write",
                "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/userinfo.profile"
              ).asJava
            )
            .build()
        )
        .setMetadata {
          val mb = Metadata
            .newBuilder()
            .addItems(Items.newBuilder().setKey("startup-script").setValue(startupScriptContent).build())
            .addItems(Items.newBuilder().setKey("google-logging-enabled").setValue("true").build())
            .addItems(Items.newBuilder().setKey("gcp_batch_service_account_email").setValue(gcpBatchSa).build())
            .addItems(Items.newBuilder().setKey("persistent-volume-size").setValue(pvSize).build())
            .addItems(Items.newBuilder().setKey("restore_galaxy").setValue(restore.toString).build())
            .addItems(Items.newBuilder().setKey("git-repo").setValue(config.galaxyVmConfig.gitRepo).build())
            .addItems(Items.newBuilder().setKey("git-branch").setValue(config.galaxyVmConfig.gitBranch).build())
            .addItems(Items.newBuilder().setKey("gcp-region").setValue(regionParam.value).build())
            .addItems(Items.newBuilder().setKey("gcp-network").setValue(network.value).build())
            .addItems(Items.newBuilder().setKey("gcp-subnet").setValue(subnetwork.value).build())
            .addItems(
              Items
                .newBuilder()
                .setKey("terra-workspace")
                .setValue(app.customEnvironmentVariables.getOrElse(WORKSPACE_NAME_KEY, ""))
                .build()
            )
            .addItems(
              Items
                .newBuilder()
                .setKey("terra-namespace")
                .setValue(app.customEnvironmentVariables.getOrElse(WORKSPACE_NAMESPACE_KEY, ""))
                .build()
            )
            .addItems(Items.newBuilder().setKey("gcp-project-id").setValue(googleProject.value).build())
            .addItems(Items.newBuilder().setKey("terra-drs-url").setValue(config.galaxyVmConfig.drsUrl).build())
            .addItems(Items.newBuilder().setKey("terra-api-url").setValue(config.galaxyVmConfig.orchUrl).build())
            // Galaxy admin user email — used by the post-install job to create the initial Galaxy admin.
            .addItems(
              Items.newBuilder().setKey("galaxy-user-email").setValue(app.auditInfo.creator.value).build()
            )
            // Leo proxy path prefix passed to ansible-pull as galaxy_prefix so Galaxy's nginx ingress
            // is configured at the correct subpath. Without this, Galaxy generates links rooted at /
            // which the browser resolves against Leo's host and gets 404s → blank page.
            .addItems(
              Items
                .newBuilder()
                .setKey("galaxy-url-prefix")
                .setValue(galaxyUrlPrefix)
                .build()
            )
            // Full HTTPS base URL of the Leo proxy (scheme + host). Combined with galaxy-url-prefix
            // in the startup script to form galaxy_infrastructure_url, which tells Galaxy to generate
            // https:// absolute links. Without this Galaxy uses its internal http:// connection and
            // produces http:// links (e.g. history export URLs) that browsers reject for the Secure cookie.
            .addItems(
              Items
                .newBuilder()
                .setKey("galaxy-proxy-base-url")
                .setValue(config.leoUrlBase.toString.stripSuffix("/"))
                .build()
            )
          // Inject TLS material for nginx. galaxy-k8s-boot fails the play on GCE when
          // galaxy_tls_cert/galaxy_tls_key are absent, so these are always expected to be set.
          for (t <- config.galaxyVmConfig.tls) {
            val (vmCert, vmKey) = generateVmTlsCert(t.caCert, t.caKey)
            mb.addItems(Items.newBuilder().setKey("galaxy_tls_cert").setValue(vmCert).build())
            mb.addItems(Items.newBuilder().setKey("galaxy_tls_key").setValue(vmKey).build())
            // nginx uses this to verify Leo's client cert in the mTLS handshake.
            mb.addItems(Items.newBuilder().setKey("galaxy_tls_client_ca").setValue(t.caCert).build())
          }
          mb.build()
        }
        .putAllLabels(Map("leonardo" -> "true").asJava)
        .build()

      // Wait for the GCP operation to complete and surface any provisioning error immediately.
      // Without this, a failure like ZONE_RESOURCE_POOL_EXHAUSTED goes undetected and Leo
      // polls for 30 minutes before giving up.
      // On zone exhaustion, retry up to 2 times with a 30s delay — capacity can free up quickly.
      // All other errors fail immediately. Machine type and zone are unchanged across retries
      // so the user sees no cost or configuration surprise.
      _ <- {
        def tryCreate(attemptsLeft: Int): F[Unit] = {
          val createAndWait: F[Unit] = for {
            opOpt <- computeService.createInstance(googleProject, zoneParam, instance)
            _ <- opOpt.traverse_(op => F.blocking(op.get()).void)
          } yield ()
          createAndWait.recoverWith { case e: java.util.concurrent.ExecutionException =>
            val cause = Option(e.getCause).getOrElse(e)
            val gcpMsg = Option(cause.getMessage).getOrElse(cause.getClass.getName)
            // GCP surfaces ZONE_RESOURCE_POOL_EXHAUSTED as UNAVAILABLE (HTTP 503) at the transport
            // layer, so both strings indicate the zone has no capacity for this machine type.
            val isCapacityError =
              gcpMsg.contains("ZONE_RESOURCE_POOL_EXHAUSTED") || gcpMsg.contains("UNAVAILABLE")
            if (isCapacityError && attemptsLeft > 0)
              logger.warn(ctx.loggingCtx)(
                s"Galaxy VM ${instanceName.value}: no ${vmMachineType.value} capacity in ${zoneParam.value} ($attemptsLeft attempt(s) remaining), retrying in 30s"
              ) >> F.sleep(30.seconds) >> tryCreate(attemptsLeft - 1)
            else {
              val userMsg =
                if (isCapacityError)
                  s"Galaxy VM ${instanceName.value} failed to provision: no ${vmMachineType.value} capacity available in ${zoneParam.value}. Try creating the app with a different machine type."
                else
                  s"Galaxy VM ${instanceName.value} failed to provision: $gcpMsg"
              logger.error(ctx.loggingCtx)(
                s"Galaxy VM ${instanceName.value} GCP create-instance operation failed: $gcpMsg"
              ) >> F.raiseError[Unit](AppCreationException(userMsg, traceId = Some(ctx.traceId)))
            }
          }
        }
        tryCreate(2)
      }

      // Grant the pet SA permission to submit and monitor GCP Batch jobs in this project.
      // Galaxy uses the VM's attached SA (pet SA) to call the Batch API.
      _ <- {
        val call = F.fromFuture(
          F.delay(
            googleIamDAO
              .addRoles(googleProject,
                        app.googleServiceAccount,
                        IamMemberTypes.ServiceAccount,
                        Set("roles/batch.jobsEditor")
              )
              .void
          )
        )
        val retryConfig = RetryPredicates.retryConfigWithPredicates(when409, whenGroupDoesNotExist)
        tracedRetryF(retryConfig)(
          call,
          s"googleIamDAO.addRoles(batch.jobsEditor) for pet SA ${app.googleServiceAccount.value} in project ${googleProject.value}"
        ).compile.lastOrError
      }

      // Grant the pet SA serviceAccountUser on the Batch SA so it can specify it as the job runner identity.
      // Only attempted when the Batch SA lives in the same project as the user (i.e. not a shared platform SA).
      // For cross-project Batch SAs, this binding must be set up externally (e.g. via Terraform).
      gcpBatchSaProject = GoogleProject(
        gcpBatchSa.split("@").lift(1).map(_.stripSuffix(".iam.gserviceaccount.com")).getOrElse("")
      )
      _ <-
        if (gcpBatchSaProject == googleProject)
          F.fromFuture(
            F.delay(
              googleIamDAO.addIamPolicyBindingOnServiceAccount(
                googleProject,
                WorkbenchEmail(gcpBatchSa),
                app.googleServiceAccount,
                Set("roles/iam.serviceAccountUser")
              )
            )
          )
        else
          logger.info(ctx.loggingCtx)(
            s"Batch SA $gcpBatchSa is in a different project ($gcpBatchSaProject) than ${googleProject.value}; " +
              s"skipping serviceAccountUser binding — must be configured externally"
          )

      // Grant the Batch SA the project-level roles it needs to submit and report on Batch jobs.
      // roles/iam.serviceAccountUser is intentionally NOT granted at the project level — a
      // project-scoped binding would let the batch SA impersonate any SA in the project, enabling
      // cross-user token theft via the metadata server of a secondary Batch VM. Instead we grant
      // it resource-level serviceAccountUser on itself below so it can only run jobs as itself.
      // See https://github.com/galaxyproject/galaxy-k8s-boot?tab=readme-ov-file#prerequisites
      _ <- {
        val call = F.fromFuture(
          F.delay(
            googleIamDAO
              .addRoles(
                googleProject,
                WorkbenchEmail(gcpBatchSa),
                IamMemberTypes.ServiceAccount,
                Set("roles/batch.jobsEditor", "roles/batch.agentReporter", "roles/logging.logWriter")
              )
              .void
          )
        )
        val retryConfig = RetryPredicates.retryConfigWithPredicates(when409, whenGroupDoesNotExist)
        tracedRetryF(retryConfig)(
          call,
          s"googleIamDAO.addRoles(batch.jobsEditor, batch.agentReporter, logging.logWriter) for Batch SA $gcpBatchSa in project ${googleProject.value}"
        ).compile.lastOrError
      }

      // Grant the Batch SA serviceAccountUser on itself so it can submit sub-jobs running as itself.
      // Scoped to the SA resource — not the project — to prevent impersonation of other SAs.
      _ <- F.fromFuture(
        F.delay(
          googleIamDAO.addIamPolicyBindingOnServiceAccount(
            googleProject,
            WorkbenchEmail(gcpBatchSa),
            WorkbenchEmail(gcpBatchSa),
            Set("roles/iam.serviceAccountUser")
          )
        )
      )

      // Create an NFS firewall rule so GCP Batch VMs can reach the Galaxy VM's NFS server.
      // Idempotent: skipped if the rule already exists.
      nfsFwName = FirewallRuleName("leonardo-galaxy-allow-nfs-for-batch")
      nfsFwExists <- computeService.getFirewallRule(googleProject, nfsFwName)
      _ <-
        if (nfsFwExists.isEmpty) {
          val nfsFirewall = Firewall
            .newBuilder()
            .setName(nfsFwName.value)
            .setNetwork(s"projects/${googleProject.value}/global/networks/${network.value}")
            .addSourceRanges("10.0.0.0/8")
            .addTargetTags(config.vpcNetworkTag.value)
            .addAllowed(Allowed.newBuilder().setIPProtocol("tcp").addPorts("2049").build())
            .addAllowed(Allowed.newBuilder().setIPProtocol("udp").addPorts("2049").build())
            .addAllowed(Allowed.newBuilder().setIPProtocol("tcp").addPorts("111").build())
            .addAllowed(Allowed.newBuilder().setIPProtocol("udp").addPorts("111").build())
            .build()
          computeService
            .addFirewallRule(googleProject, nfsFirewall)
            .flatMap(op => F.blocking(op.get()).void)
        } else
          logger.info(ctx.loggingCtx)(
            s"NFS firewall rule ${nfsFwName.value} already exists, skipping creation"
          )

      _ <- logger.info(ctx.loggingCtx)(
        s"Galaxy VM instance ${instanceName.value} GCP create operation complete; polling for external IP"
      )

      // Poll until the instance has an external IP assigned (needed for both proxy routing and readiness check).
      // We store the external IP because Leo's GKE cluster and the Galaxy VM are in different GCP projects
      // whose VPCs are not peered, making the internal IP unreachable from Leo's pod.
      ipPairOpt <- streamFUntilDone(
        computeService.getInstance(googleProject, zoneParam, instanceName).flatMap { instanceOpt =>
          instanceOpt match {
            case None =>
              logger
                .debug(ctx.loggingCtx)(
                  s"Galaxy VM ${instanceName.value}: not yet visible in GCE API; will retry"
                )
                .as(Option.empty[(IP, IP)])
            case Some(inst) =>
              val ipPair = for {
                iface <- Option(inst.getNetworkInterfacesList).flatMap(_.asScala.headOption)
                internalIp = IP(iface.getNetworkIP)
                cfg <- Option(iface.getAccessConfigsList).flatMap(_.asScala.headOption)
                natIp <- Option(cfg.getNatIP).filter(_.nonEmpty)
              } yield (internalIp, IP(natIp))
              ipPair match {
                case None =>
                  logger
                    .debug(ctx.loggingCtx)(
                      s"Galaxy VM ${instanceName.value}: instance exists but external NAT IP not yet assigned; will retry"
                    )
                    .as(Option.empty[(IP, IP)])
                case some => F.pure(some)
              }
          }
        },
        config.monitorConfig.createApp.maxAttempts,
        config.monitorConfig.createApp.interval
      ).compile.lastOrError

      (internalIp, externalIp) <- F.fromOption(
        ipPairOpt,
        AppCreationException(
          s"Galaxy VM ${instanceName.value} did not obtain an IP after ${config.monitorConfig.createApp.interruptAfter}",
          traceId = Some(ctx.traceId)
        )
      )

      _ <- logger.info(ctx.loggingCtx)(
        s"Galaxy VM ${instanceName.value} has internal IP ${internalIp.asString} / external IP ${externalIp.asString}; storing external IP in cluster async fields for proxy access"
      )

      // Store the VM's external IP as the cluster load balancer IP consumed by KubernetesDnsCache.
      // The proxy connects via opportunistic TLS on port 443 (Galaxy VM's nginx serves HTTPS with
      // a self-signed cert generated at cloud-init time). We use the external IP because Leo's
      // GKE cluster is in Leo's GCP project while the Galaxy VM is in the user's workspace project
      // — the two VPCs are not peered, so the internal IP is not routable from Leo's pod. The
      // leonardo-allow-https firewall rule (port 443, targeting VMs with the "leonardo" tag)
      // allows Leo to reach the VM on its external IP.
      _ <- kubernetesClusterQuery
        .updateAsyncFields(
          dbCluster.id,
          KubernetesClusterAsyncFields(
            externalIp,
            IP(""),
            NetworkFields(NetworkName(""), SubnetworkName(""), IpRange(""))
          )
        )
        .transaction
      _ <- kubernetesClusterQuery.updateStatus(dbCluster.id, KubernetesClusterStatus.Running).transaction

      _ <- logger.info(ctx.loggingCtx)(
        s"Polling Galaxy readiness for app ${app.appName.value} via proxy (backend: ${externalIp.asString}:443)"
      )

      // Poll /api/version rather than the bare prefix path: nginx can return a 301 redirect
      // (trailing-slash normalisation) for the bare path while Galaxy pods are still starting,
      // which would satisfy < 400 and mark the app Running too early. /api/version requires
      // Galaxy's Python API to be fully initialised and only returns 200 at that point.
      isDone <- streamFUntilDone(
        appDao.isVmReachable(externalIp, 443, ctx.traceId, s"$galaxyUrlPrefix/api/version"),
        config.monitorConfig.createApp.maxAttempts,
        config.monitorConfig.createApp.interval
      ).interruptAfter(config.monitorConfig.createApp.interruptAfter).compile.lastOrError

      _ <-
        if (!isDone) {
          val msg =
            s"Galaxy VM installation has failed or timed out for app ${app.appName.value} in project ${googleProject.value}"
          logger.error(ctx.loggingCtx)(msg) >>
            F.raiseError[Unit](AppCreationException(msg, traceId = Some(ctx.traceId)))
        } else F.unit

    } yield ()

  /** Deletes the Galaxy VM and marks the app's placeholder cluster row as deleted. Disks are kept. */
  private[util] def deleteGalaxyVm(dbApp: GetAppResult)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      app = dbApp.app
      dbCluster = dbApp.cluster
      gp <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(dbCluster.cloudContext),
        new RuntimeException("Galaxy app cloud context should be a google project")
      )
      instanceName = InstanceName(s"galaxy-${app.appName.value}")
      zone = app.appResources.disk.map(_.zone).getOrElse(ZoneName(config.clusterConfig.location.value))
      _ <- computeService
        .deleteInstance(gp, zone, instanceName)
        .void
        .handleErrorWith { e =>
          logger.warn(ctx.loggingCtx)(
            s"Failed to delete Galaxy VM ${instanceName.value}: ${e.getMessage}. Continuing with app deletion."
          )
        }
      // No separate cluster-deletion message is sent for Galaxy, so the row is marked here.
      // Otherwise it would block future app creation in this project.
      _ <- kubernetesClusterQuery.markAsDeleted(dbCluster.id, ctx.now).transaction
    } yield ()

  /** Generates a fresh RSA key pair and an X.509 cert signed by Leo's root CA.
   *  Returns (certPem, keyPem). Called once per Galaxy VM at provisioning time so
   *  each VM gets a unique key — a user reading their VM's metadata can only
   *  compromise their own VM, not others.
   */
  private[util] def generateVmTlsCert(caCertPem: String, caKeyPem: String): (String, String) = {
    // Parse CA cert
    val caCert = CertificateFactory
      .getInstance("X.509")
      .generateCertificate(new ByteArrayInputStream(caCertPem.getBytes))
      .asInstanceOf[X509Certificate]

    // Parse CA private key (PKCS#1 RSA PEM from Terraform tls_private_key)
    val pemParser = new PEMParser(new StringReader(caKeyPem))
    val caPrivateKey =
      new JcaPEMKeyConverter().getKeyPair(pemParser.readObject().asInstanceOf[PEMKeyPair]).getPrivate
    pemParser.close()

    // Generate per-VM RSA key pair
    val keyGen = KeyPairGenerator.getInstance("RSA")
    keyGen.initialize(2048, new SecureRandom())
    val vmKeyPair = keyGen.generateKeyPair()

    // Build cert signed by CA.
    // Use the CA cert's subject field verbatim (via JcaX509CertificateHolder) so the issuer DER
    // byte sequence in the VM cert matches the CA subject exactly. X500Name(String) re-encodes
    // from RFC 2253, which can reorder attributes and break PKIX issuer/subject matching.
    val now = Instant.now()
    val certBuilder = new JcaX509v3CertificateBuilder(
      new JcaX509CertificateHolder(caCert).getSubject,
      BigInteger.valueOf(now.toEpochMilli),
      Date.from(now),
      Date.from(now.plus(Duration.ofDays(365 * 5))),
      new X500Name("CN=galaxy-vm.internal"),
      vmKeyPair.getPublic
    )
    val signer = new JcaContentSignerBuilder("SHA256WithRSA").build(caPrivateKey)
    val vmCert = new JcaX509CertificateConverter().getCertificate(certBuilder.build(signer))

    // Serialize to PEM strings
    def toPem(obj: Object): String = {
      val sw = new StringWriter()
      val w = new JcaPEMWriter(sw)
      w.writeObject(obj)
      w.close()
      sw.toString
    }

    (toPem(vmCert), toPem(vmKeyPair.getPrivate))
  }
}

object GalaxyVmInterpreter {
  private[leonardo] def getOldStyleGalaxyPostgresDiskName(namespaceName: NamespaceName, suffix: String): DiskName =
    DiskName(s"${namespaceName.value}-${suffix}")

  private[leonardo] def getGalaxyPostgresDiskName(dataDiskName: DiskName, suffix: String): DiskName =
    DiskName(s"${dataDiskName.value}-${suffix}")

  private[leonardo] def buildGalaxyPostgresDisk(zone: ZoneName,
                                                dataDiskName: DiskName,
                                                galaxyDiskConfig: GalaxyDiskConfig
  ): Disk =
    Disk
      .newBuilder()
      .setName(getGalaxyPostgresDiskName(dataDiskName, galaxyDiskConfig.postgresDiskNameSuffix).value)
      .setZone(zone.value)
      .setSizeGb(galaxyDiskConfig.postgresDiskSizeGB.gb)
      .setPhysicalBlockSizeBytes(galaxyDiskConfig.postgresDiskBlockSize.bytes)
      .putAllLabels(Map("leonardo" -> "true").asJava)
      .build()
}
