package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, ContainerImage}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.TraceId
import org.http4s.Uri

trait AppDescriptorDAO[F[_]] {
  def getDescriptor(path: Uri)(implicit ev: Ask[F, AppContext]): F[AppDescriptor]
}

// This AppDescriptor and subsequent CustomAppService model are sourced from the app.yaml schema detailed here
// https://github.com/DataBiosphere/terra-app#app-schema
// Any changes to this schema should be sourced from this link
final case class AppDescriptor(name: String,
                               author: String,
                               description: String,
                               version: String,
                               services: Map[String, CustomAppService]
)

final case class CustomAppService(image: ContainerImage,
                                  port: Int,
                                  baseUrl: String,
                                  command: List[String],
                                  args: List[String],
                                  pdMountPath: String,
                                  pdAccessMode: String,
                                  environment: Map[String, String]
)

final case class AppDescriptorException(traceId: TraceId, path: String, msg: String)
    extends LeoException(message = s"Error occurred fetching app descriptor from path $path: $msg",
                         traceId = Some(traceId)
    )
