package org.broadinstitute.dsde.workbench.config

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory

import scala.collection.JavaConverters._

object AuthTokens {
  val dumbledore = AuthToken(WorkbenchConfig.Users.dumbledore)
  val admin = dumbledore
  val hermione = AuthToken(WorkbenchConfig.Users.hermione)
  val owner = hermione
  val mcgonagall = AuthToken(WorkbenchConfig.Users.mcgonagall)
  val curator = mcgonagall
  val harry = AuthToken(WorkbenchConfig.Users.harry)
  val testUser = harry
  val dominique = harry
  val fred = AuthToken(WorkbenchConfig.Users.fred)
  val elvin = fred
  val george = AuthToken(WorkbenchConfig.Users.george)
  val bill = AuthToken(WorkbenchConfig.Users.bill)
  val lunaTemp = AuthToken(WorkbenchConfig.Users.lunaTemp)
  val nevilleTemp = AuthToken(WorkbenchConfig.Users.nevilleTemp)
}

case class AuthToken(value: String)

case object AuthToken {
  def apply(user: Credentials): AuthToken = getUserToken(user.email)

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = JacksonFactory.getDefaultInstance
  val authScopes = Seq("profile", "email", "openid", "https://www.googleapis.com/auth/devstorage.full_control", "https://www.googleapis.com/auth/cloud-platform")

  def getUserToken(userEmail: String): AuthToken = {
    val cred = buildCredential(userEmail)
    cred.refreshToken()
    AuthToken(cred.getAccessToken)
  }

  def buildCredential(userEmail: String): GoogleCredential = {
    val pemfile = new java.io.File(WorkbenchConfig.GCS.pathToQAPem)
    val email = WorkbenchConfig.GCS.qaEmail

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


