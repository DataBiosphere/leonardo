package fs2
package io
package net

import cats.effect.kernel.Resource
import com.comcast.ip4s.{Dns, GenSocketAddress, Host, IpAddress, NetworkInterfaces, Port, SocketAddress}
import fs2.io.net.tls.TLSContext

import java.util.concurrent.ThreadFactory
import scala.annotation.nowarn

/** A `Network[F]` that delegates everything to `underlying`, except that the address passed to
  * `connect` is first transformed by `rewrite`.
  *
  * http4s Ember ignores `withSocketGroup` (a no-op since 0.23.34 / 1.0.0-M47) and exposes no
  * resolver hook, so supplying a custom `Network` to `EmberClientBuilder.default` is the only way
  * to redirect TCP connections. fs2 offers no pluggable DNS either: `Network.forAsyncAndDns`
  * discards its `Dns` argument (fs2 3.14). Since fs2 3.13 `Network` is sealed, so this file lives
  * in package `fs2.io.net` to extend `Network.UnsealedNetwork`, which is `private[fs2]`. Expect to
  * revisit it when an fs2 upgrade adds abstract members to `Network`.
  */
@nowarn("cat=deprecation")
final class AddressRewritingNetwork[F[_]](
  underlying: Network[F],
  rewrite: GenSocketAddress => F[GenSocketAddress]
) extends Network.UnsealedNetwork[F] {

  override def connect(address: GenSocketAddress, options: List[SocketOption]): Resource[F, Socket[F]] =
    Resource.eval(rewrite(address)).flatMap(underlying.connect(_, options))

  override def bind(address: GenSocketAddress, options: List[SocketOption]): Resource[F, ServerSocket[F]] =
    underlying.bind(address, options)

  override def bindAndAccept(address: GenSocketAddress, options: List[SocketOption]): Stream[F, Socket[F]] =
    underlying.bindAndAccept(address, options)

  override def bindDatagramSocket(address: GenSocketAddress,
                                  options: List[SocketOption]
  ): Resource[F, DatagramSocket[F]] =
    underlying.bindDatagramSocket(address, options)

  override def tlsContext: TLSContext.Builder[F] = underlying.tlsContext
  override def dns: Dns[F] = underlying.dns
  override def interfaces: NetworkInterfaces[F] = underlying.interfaces

  // Deprecated operations, still abstract on Network. Route client through the rewrite too.

  override def client(to: SocketAddress[Host], options: List[SocketOption]): Resource[F, Socket[F]] =
    connect(to, options)

  override def server(address: Option[Host], port: Option[Port], options: List[SocketOption]): Stream[F, Socket[F]] =
    underlying.server(address, port, options)

  override def serverResource(address: Option[Host],
                              port: Option[Port],
                              options: List[SocketOption]
  ): Resource[F, (SocketAddress[IpAddress], Stream[F, Socket[F]])] =
    underlying.serverResource(address, port, options)

  override def openDatagramSocket(address: Option[Host],
                                  port: Option[Port],
                                  options: List[DatagramSocketOption],
                                  protocolFamily: Option[DatagramSocketGroup.ProtocolFamily]
  ): Resource[F, DatagramSocket[F]] =
    underlying.openDatagramSocket(address, port, options, protocolFamily)

  override def socketGroup(threadCount: Int, threadFactory: ThreadFactory): Resource[F, SocketGroup[F]] =
    Resource.pure(this)

  override def datagramSocketGroup(threadFactory: ThreadFactory): Resource[F, DatagramSocketGroup[F]] =
    Resource.pure(this)
}
