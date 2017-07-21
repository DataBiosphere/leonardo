package org.broadinstitute.dsde.workbench

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.dataproc.model.Cluster
import org.broadinstitute.dsde.workbench.google.GoogleUtilities
import org.broadinstitute.dsde.workbench.model.ErrorReportSource

import scala.concurrent.ExecutionContext

package object leonardo {
  implicit val errorReportSource = ErrorReportSource("leonardo")
}


// don't really need materializer

class leo(implicit val system: ActorSystem, val executionContext: ExecutionContext) extends GoogleUtilities{

  def getDataProcServiceAccountCredential: Credential = {
    new GoogleCredential.Builder().build()
  }


  def build() = {
    val dataproc = new Dataproc.Builder(GoogleNetHttpTransport.newTrustedTransport, JacksonFactory.getDefaultInstance, getDataProcServiceAccountCredential)
      .setApplicationName("dataproc").build()
    val request = dataproc.projects().regions().clusters().create("project", "region", new Cluster())

    executeGoogleRequest(request)
  }


}
