package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{IO, Resource}
import com.google.cloud.oslogin.common.OsLoginProto.SshPublicKey
import com.google.cloud.oslogin.v1.{ImportSshPublicKeyRequest, OsLoginServiceClient}
import org.broadinstitute.dsde.rawls.model.AzureManagedAppCoordinates
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.connection.channel.direct.Session
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.sys.process._

case class TunnelName(value: String) extends AnyVal
case class ResourceGroup(value: String)

case class Tunnel(pid: String, port: Int) {
  val hostName = "127.0.0.1"
}

object SSH {
  val loggerIO: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  // TODO: If multiple tests need to ssh into an azure VM: add a lock of sorts, only one tunnel at a time with same port
  // A bastion tunnel is needed to tunnel to an azure vm
  // See: https://learn.microsoft.com/en-us/azure/bastion/native-client
  def startAzureBastionTunnel(runtimeName: RuntimeName, port: Int = LeonardoConfig.Azure.defaultBastionPort)(implicit
    staticTestCoordinates: AzureManagedAppCoordinates
  ): Resource[IO, Tunnel] = {
    val targetResourceId =
      s"/subscriptions/${staticTestCoordinates.subscriptionId.toString}/resourceGroups/${staticTestCoordinates.managedResourceGroupId}/providers/Microsoft.Compute/virtualMachines/${runtimeName.asString}"

    val makeTunnel = for {
      scriptPath <- IO(getClass.getClassLoader.getResource("startTunnel.sh").getPath)
      process = Process(
        scriptPath,
        None,
        "BASTION_NAME" -> LeonardoConfig.Azure.bastionName,
        "RESOURCE_GROUP" -> staticTestCoordinates.managedResourceGroupId,
        "RESOURCE_ID" -> targetResourceId,
        "PORT" -> port.toString
      )
      _ <- loggerIO.info(s"startTunnel process")
      processString = process.toString
      _ <- loggerIO.info(s"startTunnel process string: ${processString}")
      hasExit = process.hasExitValue
      _ <- loggerIO.info(s"startTunnel process hasExit: ${hasExit}")
      processLazy = process.lazyLines
      pid = "1632"
//      output <- IO(process)
      _ <- loggerIO.info(s"Bastion tunnel start command pid output:\n\t${pid}")
      tunnel = Tunnel(pid, port)
    } yield tunnel

    Resource.make(makeTunnel)(tunnel => loggerIO.info("Closing tunnel") >> closeTunnel(tunnel))
  }

  final case class SSHSession(session: Session, client: SSHClient)
  // Note that a session is a one time use resource, and only supports one command execution
  // This method starts an ssh session to either an azure or google runtime.
  // However, it is currently only used for azure ssh due to system limitations
  // Specifically, azure can use username/password auth to ssh to the vm (which is specified in WSM at creation time and in vault)
  // For google, this method generates public/private keys and uploads the public key to the qa service account's OSlogin registry in the test project
  // This works when you can connect directly to the vm, but the tests do not necessarily run on broad internal IP space
  // As such, we use `executeGoogleCommand` for interacting with google VMs typically.
  private def startSSHConnection(hostName: String, port: Int, sshConfig: SSHRuntimeInfo): Resource[IO, SSHSession] = {
    val sessionAndClient = for {
      client <- IO(new SSHClient)
      _ <- loggerIO.info(s"Adding host key verifier for shh client}")
      _ <- IO(client.addHostKeyVerifier(new PromiscuousVerifier()))

      _ <- loggerIO.info(s"Connecting via ssh client hostname ${hostName} port $port")
      _ <- IO(client.connect(hostName, port))

      _ <- loggerIO.info("Authenticating ssh client ")
      _ <-
        if (sshConfig.cloudProvider == CloudProvider.Azure)
          IO(client.authPassword(LeonardoConfig.Azure.vmUser, LeonardoConfig.Azure.vmPassword))
        else {
          for {
            keyConfig <- createSSHKeys(WorkbenchEmail(LeonardoConfig.Leonardo.serviceAccountEmail),
                                       sshConfig.googleProject.get
            )
            _ <- IO(client.authPublickey(keyConfig.username, keyConfig.privateKey.toAbsolutePath.toString))
          } yield ()
        }

      _ <- loggerIO.info("Starting ssh session")
      session <- IO(client.startSession())
    } yield SSHSession(session, client)

    Resource.make(sessionAndClient)(sessionAndClient =>
      loggerIO.info(s"cleaning up session for port ${port}") >> IO(
        sessionAndClient.session.close()
      ) >> IO(
        sessionAndClient.client.disconnect()
      )
    )
  }

  final case class SSHKeyConfig(username: String, publicKey: String, privateKey: Path)
  private def createSSHKeys(serviceAccount: WorkbenchEmail, googleProject: GoogleProject): IO[SSHKeyConfig] = {
    val privateKeyFileName = s"/tmp/key-${UUID.randomUUID().toString.take(8)}"
    val createKeysCmd =
      s"ssh-keygen -t rsa -N '' -f $privateKeyFileName"
    for {
      output <- IO(createKeysCmd !!)
      publicKey: String = Files.readString(Paths.get(s"$privateKeyFileName.pub")).strip()
      privateKey: Path = Paths.get(privateKeyFileName)
      account = s"users/${serviceAccount.value}"

      _ <- loggerIO.info(s"about to import public key for user ${account}")

      request = ImportSshPublicKeyRequest
        .newBuilder()
        .setParent(account)
        .setSshPublicKey(SshPublicKey.newBuilder().setKey(publicKey))
        .setProjectId(googleProject.value)
        .build()

      client = OsLoginServiceClient
        .create()

      settings = client.getSettings()
      _ <- loggerIO.info(s"settings ${settings}")

      _ <- loggerIO.info("importing ssh public key")
      _ <- IO(client.importSshPublicKey(request))

    } yield SSHKeyConfig(LeonardoConfig.GCS.leonardoServiceAccountUsername, publicKey, privateKey)
  }

  final case class SSHRuntimeInfo(googleProject: Option[GoogleProject], cloudProvider: CloudProvider)
  def startSessionAndExecuteCommand(hostName: String,
                                    port: Int,
                                    command: String,
                                    sshConfig: SSHRuntimeInfo
  ): IO[CommandResult] =
    for {
      output <- SSH.startSSHConnection(hostName, port, sshConfig).use { connection =>
        executeCommand(connection.session, command)
      }
    } yield output

  // This method is the main one for any interaction with GCP VMs
  def executeGoogleCommand(project: GoogleProject, zone: String, runtimeName: RuntimeName, cmd: String): IO[String] = {
    val dummyCommand =
      s"gcloud compute ssh --zone '${zone}' '${LeonardoConfig.GCS.leonardoServiceAccountUsername}@${runtimeName.asString}' --project '${project.value}' --tunnel-through-iap -q --command=\"ls\" -- -tt"

    // This command is a bit special.
    // It needs to go through 4 layers of interpolation:
    // 1 scala """...""",
    // 2 bash on the GHA node,
    // 3 bash on the VM leo creates, and
    // 4 finally bash within the docker container
    //
    // Syntax for the `--command='...'` is specific to this situation and ensures desired behavior
    // For example, to use command with echo that is executed in bash at layer 3 with quotes that persist into layer 4 `sudo docker exec -it jupyter-server bash -c "echo \\\"this should save\\\" > /home/jupyter/test.txt"`
    val sshCommand =
      s"""
          gcloud compute ssh --zone '${zone}' '${LeonardoConfig.GCS.leonardoServiceAccountUsername}@${runtimeName.asString}' --project '${project.value}' --tunnel-through-iap -q --verbosity=error --command='$cmd' -- -tt
      """

    val outWriter = new StringBuilder
    val errWriter = new StringBuilder
    val logger = ProcessLogger((out: String) => outWriter.append(out), (err: String) => errWriter.append(err))

    for {
      // Without first executing a dummy command, the output will contain a bunch of garble that google spits out because gcloud compute ssh runs ssh-keygen under the covers
      // Unfortunately, they provide a way to pass args to the subsequent ssh call but not the keygen call, so we need to always execute a command and throw away the output before doing meaningful work
      _ <- loggerIO.debug(s"executing dummy command to generate ssh keys...")
      dummyOutput <- IO(dummyCommand !!)
      _ <- loggerIO.debug(s"dummy command output: \n\t$dummyOutput")
      _ <- loggerIO.info(s"executing command: \n\t$sshCommand")
      exitCode <- IO(sshCommand.!(logger))
      output = outWriter.toString
      error = errWriter.toString
      _ <-
        if (exitCode == 0) loggerIO.info(s"cmd output: \n\tcmd: $sshCommand \n\toutput: $output")
        else
          loggerIO.error(
            s"error occurred during command execution. \n\tcmd: $sshCommand \n\tstdOut: $output \n\tstdErr: $error"
          )
    } yield output
  }

  private def closeTunnel(tunnel: Tunnel): IO[Unit] =
    loggerIO.info(s"Killing tunnel via pid ${tunnel.pid}") >> IO(s"kill ${tunnel.pid}" !!)

  // Exec docs/examples: https://www.tabnine.com/code/java/methods/net.schmizz.sshj.connection.channel.direct.Session/exec
  private def executeCommand(session: Session, cmd: String): IO[CommandResult] =
    for {
      _ <- loggerIO.info("beginning to execute command")
      _ <- IO(session.allocateDefaultPTY())
      cmd <- IO(session.exec(cmd))
      (code, inputStream) = (cmd.getExitStatus(), cmd.getInputStream)
      outputLines = scala.io.Source.fromInputStream(inputStream).getLines().toList
      _ <- loggerIO.info("cmd output from exec:" + outputLines)
      _ <- IO(cmd.join(10, TimeUnit.SECONDS))
    } yield CommandResult(code, outputLines)

}

final case class CommandResult(exitCode: Int, outputLines: List[String])
