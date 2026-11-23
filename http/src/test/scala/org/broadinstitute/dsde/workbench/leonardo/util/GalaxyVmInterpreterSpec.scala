package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.google2.mock.{FakeGoogleComputeService, FakeGoogleResourceService}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockAppDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{kubernetesClusterQuery, KubernetesServiceDbQueries, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x500.style.{BCStyle, IETFUtils}
import org.bouncycastle.cert.jcajce.{JcaX509CertificateConverter, JcaX509CertificateHolder, JcaX509v3CertificateBuilder}
import org.bouncycastle.openssl.jcajce.{JcaPEMKeyConverter, JcaPEMWriter}
import org.bouncycastle.openssl.{PEMKeyPair, PEMParser}
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.scalatest.flatspec.AnyFlatSpecLike

import java.io.{ByteArrayInputStream, StringReader, StringWriter}
import java.math.BigInteger
import java.security.cert.{CertificateFactory, X509Certificate}
import java.security.interfaces.RSAPublicKey
import java.security.{KeyPair, KeyPairGenerator}
import java.time.{Duration, Instant}
import java.util.Date
import scala.concurrent.ExecutionContext.Implicits.global

class GalaxyVmInterpreterSpec extends AnyFlatSpecLike with TestComponent with LeonardoTestSuite {
  val vpcInterp =
    new VPCInterpreter[IO](Config.vpcInterpreterConfig, FakeGoogleResourceService, FakeGoogleComputeService)

  val galaxyVmInterp = new GalaxyVmInterpreter[IO](
    Config.gkeInterpConfig,
    vpcInterp,
    new MockGoogleIamDAO,
    FakeGoogleComputeService,
    MockAppDAO
  )

  private def toPem(obj: Object): String = {
    val sw = new StringWriter()
    val w = new JcaPEMWriter(sw)
    w.writeObject(obj)
    w.close()
    sw.toString
  }

  private def parseCert(pem: String): X509Certificate =
    CertificateFactory
      .getInstance("X.509")
      .generateCertificate(new ByteArrayInputStream(pem.getBytes))
      .asInstanceOf[X509Certificate]

  // Self-signed test CA, with the key written as a PKCS#1 PEM like the Terraform-managed CA.
  private def makeCa(): (X509Certificate, String, String) = {
    val keyGen = KeyPairGenerator.getInstance("RSA")
    keyGen.initialize(2048)
    val caKeyPair: KeyPair = keyGen.generateKeyPair()
    val caName = new X500Name("CN=Test Galaxy CA,O=Leonardo Test")
    val now = Instant.now()
    val builder = new JcaX509v3CertificateBuilder(
      caName,
      BigInteger.ONE,
      Date.from(now),
      Date.from(now.plus(Duration.ofDays(30))),
      caName,
      caKeyPair.getPublic
    )
    val signer = new JcaContentSignerBuilder("SHA256WithRSA").build(caKeyPair.getPrivate)
    val caCert = new JcaX509CertificateConverter().getCertificate(builder.build(signer))
    (caCert, toPem(caCert), toPem(caKeyPair))
  }

  "generateVmTlsCert" should "return a cert signed by the CA, with the CA as issuer and the VM CN" in {
    val (caCert, caCertPem, caKeyPem) = makeCa()

    val (certPem, keyPem) = galaxyVmInterp.generateVmTlsCert(caCertPem, caKeyPem)
    val vmCert = parseCert(certPem)

    noException should be thrownBy vmCert.verify(caCert.getPublicKey)
    vmCert.getIssuerX500Principal shouldBe caCert.getSubjectX500Principal
    val cn = new JcaX509CertificateHolder(vmCert).getSubject.getRDNs(BCStyle.CN).head.getFirst.getValue
    IETFUtils.valueToString(cn) shouldBe "galaxy-vm.internal"
    noException should be thrownBy vmCert.checkValidity()
    vmCert.getNotAfter.toInstant.isAfter(Instant.now().plus(Duration.ofDays(365 * 4))) shouldBe true

    val parser = new PEMParser(new StringReader(keyPem))
    val vmKey = parser.readObject() match {
      case kp: PEMKeyPair => new JcaPEMKeyConverter().getKeyPair(kp)
      case other          => fail(s"unexpected key PEM object: ${other.getClass.getName}")
    }
    parser.close()
    vmKey.getPublic.asInstanceOf[RSAPublicKey].getModulus shouldBe
      vmCert.getPublicKey.asInstanceOf[RSAPublicKey].getModulus
  }

  it should "generate a different key pair on each call" in {
    val (_, caCertPem, caKeyPem) = makeCa()

    val (cert1, key1) = galaxyVmInterp.generateVmTlsCert(caCertPem, caKeyPem)
    val (cert2, key2) = galaxyVmInterp.generateVmTlsCert(caCertPem, caKeyPem)

    key1 should not be key2
    parseCert(cert1).getPublicKey should not be parseCert(cert2).getPublicKey
  }

  "deleteGalaxyVm" should "mark the app's placeholder cluster as deleted" in isolatedDbTest {
    val res = for {
      savedCluster <- IO(makeKubeCluster(1).save())
      savedNodepool <- IO(makeNodepool(1, savedCluster.id).save())
      savedApp <- IO(makeApp(1, savedNodepool.id, appType = AppType.Galaxy).save())
      dbAppOpt <- KubernetesServiceDbQueries.getFullAppById(savedCluster.cloudContext, savedApp.id).transaction
      dbApp <- IO.fromOption(dbAppOpt)(new RuntimeException("app not found"))

      _ <- galaxyVmInterp.deleteGalaxyVm(dbApp)

      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster.id).transaction
    } yield clusterOpt.map(_.status) shouldBe Some(KubernetesClusterStatus.Deleted)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
