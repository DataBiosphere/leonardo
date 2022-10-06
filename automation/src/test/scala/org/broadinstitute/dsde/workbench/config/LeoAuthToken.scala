package org.broadinstitute.dsde.workbench.config

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.LeonardoConfig

import scala.jdk.CollectionConverters._

case class LeoAuthToken(value: String)

case object LeoAuthToken extends AuthToken {
  def apply(user: Credentials): LeoAuthToken = getUserToken(user.email)

  override val httpTransport: NetHttpTransport = GoogleNetHttpTransport.newTrustedTransport
  override val jsonFactory = com.google.api.client.json.gson.GsonFactory.getDefaultInstance
  val authScopes = Seq("profile", "email", "openid")

  def getUserToken(userEmail: String): LeoAuthToken = {
    val cred = buildCredential(userEmail)
    cred.refreshToken()
    LeoAuthToken(cred.getAccessToken)
  }

  override def buildCredential(): GoogleCredential = {
    val pemfile = new java.io.File(LeonardoConfig.GCS.pathToQAPem)
    val email = LeonardoConfig.GCS.qaEmail

    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(email)
      .setServiceAccountPrivateKeyFromPemFile(pemfile)
      .setServiceAccountScopes(authScopes.asJava)
      .setServiceAccountUser(email)
      .build()
  }

  def buildCredential(userEmail: String): GoogleCredential = {
    val pemfile = new java.io.File(LeonardoConfig.GCS.pathToQAPem)
    val email = LeonardoConfig.GCS.qaEmail

    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(email)
      .setServiceAccountPrivateKeyFromPemFile(pemfile)
      .setServiceAccountScopes(authScopes.asJava)
      .setServiceAccountUser(userEmail)
      .build()
  }
}
