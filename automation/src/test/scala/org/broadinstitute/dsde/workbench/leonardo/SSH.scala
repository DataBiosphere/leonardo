package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{IO, Resource}
import com.google.cloud.oslogin.common.OsLoginProto.SshPublicKey
import com.google.cloud.oslogin.v1.{ImportSshPublicKeyRequest, OsLoginServiceClient, GetLoginProfileRequest}
import liquibase.util.FileUtil
import org.broadinstitute.dsde.rawls.model.AzureManagedAppCoordinates
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.connection.channel.direct.Session
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.userauth.password.{PasswordFinder, ConsolePasswordFinder}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import java.io.File
import java.nio.file.{Path, Paths, Files}
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
  def startBastionTunnel(runtimeName: RuntimeName, port: Int = LeonardoConfig.Azure.defaultBastionPort)(implicit
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
      output <- IO(process !!)
      _ <- loggerIO.info(s"Bastion tunnel start command full output:\n\t${output}")
      tunnel = Tunnel(output.split('\n').last, port)
    } yield tunnel

    Resource.make(makeTunnel)(tunnel => loggerIO.info("Closing tunnel") >> closeTunnel(tunnel))
  }

  final case class SSHConnection(session: Session, client: SSHClient)
  // Note that a session is a one time use resource, and only supports one command execution
  private def startSSHConnection(hostName: String,
                                 port: Int,
                                 sshConfig: SSHRuntimeInfo
  ): Resource[IO, SSHConnection] = {
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
            keyConfig <- createSSHKeys(WorkbenchEmail(LeonardoConfig.Leonardo.serviceAccountEmail), sshConfig.googleProject.get)
            _ <- IO(client.authPublickey(keyConfig.username, keyConfig.privateKey.toAbsolutePath.toString))
          } yield ()
        }

      _ <- loggerIO.info("Starting ssh session")
      session <- IO(client.startSession())
    } yield SSHConnection(session, client)

    Resource.make(sessionAndClient)(sessionAndClient =>
      loggerIO.info(s"cleaning up tunnel and session for port ${port}") >> IO(
        sessionAndClient.session.close()
      ) >> IO(
        sessionAndClient.client.disconnect()
      )
    )
  }

  final case class SSHKeyConfig(username: String, publicKey: String, privateKey: Path)
  def createSSHKeys(serviceAccount: WorkbenchEmail, googleProject: GoogleProject): IO[SSHKeyConfig] = {
    val privateKeyFileName = s"/tmp/key-${UUID.randomUUID().toString.take(8)}"
    val createKeysCmd = s"ssh-keygen -t rsa -N '' -C 'leonardo-qa@broad-dsde-qa.iam.gserviceaccount.com' -f $privateKeyFileName"
    for {
      _ <- IO(createKeysCmd !!)
      publicKey: String = Files.readString(Paths.get(s"$privateKeyFileName.pub"))
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

      profileRequest = GetLoginProfileRequest.newBuilder().setName(account).setProjectId(googleProject.value).build()

//      _ <- loggerIO.info("getting profile for ssh client")
//      profile <- IO(client.getLoginProfile(profileRequest))
//      username = profile.getName
//      _ <- loggerIO.info(s"found username for acct ${account}")

      _ <- loggerIO.info("importing ssh public key")
      _ <- IO(client.importSshPublicKey(request))

    } yield SSHKeyConfig("sa_117004481319471038556", publicKey, privateKey)
  }

  final case class SSHRuntimeInfo(googleProject: Option[GoogleProject], cloudProvider: CloudProvider)
  def executeCommand(hostName: String, port: Int, command: String, sshConfig: SSHRuntimeInfo): IO[CommandResult] =
    for {
      output <- SSH.startSSHConnection(hostName, port, sshConfig).use { connection =>
        executeCommand(connection.session, command)
      }
    } yield output

  def closeTunnel(tunnel: Tunnel): IO[Unit] =
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
