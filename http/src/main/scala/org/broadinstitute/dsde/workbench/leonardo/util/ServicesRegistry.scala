package org.broadinstitute.dsde.workbench.leonardo.util

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag

/**
 * A simple registry for dependencies. It is used to register and look up dependencies by their type.
 * Its purpose is primarily to enable lazy look-up of services/dependencies that require instances of cloud-specific clients.
 */
trait ServicesRegistry {
  def register[F: ClassTag](service: F): Unit
  def lookup[F: ClassTag]: Option[F]

  def clear(): Unit
}

object ServicesRegistry {
  def apply(): ServicesRegistry = new ServicesRegistry {
    private val services: TrieMap[Class[_], Any] = TrieMap.empty

    override def register[F: ClassTag](service: F): Unit =
      services.putIfAbsent(implicitly[ClassTag[F]].runtimeClass, service)

    override def lookup[F: ClassTag]: Option[F] = {
      val lookUpClass = implicitly[ClassTag[F]].runtimeClass
      services.get(lookUpClass).asInstanceOf[Option[F]]
    }

    override def clear(): Unit = services.clear()
  }
}
