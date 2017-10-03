package org.broadinstitute.dsde.workbench.config

import com.typesafe.config.ConfigFactory

object WorkbenchConfig extends WorkbenchConfig

trait WorkbenchConfig {
  protected val config = ConfigFactory.load()

  private val users = config.getConfig("users")
  private val chromeSettings = config.getConfig("chromeSettings")
  private val gcsConfig = config.getConfig("gcs")

  object GCS {
    val pathToQAPem = gcsConfig.getString("qaPemFile")
    val qaEmail = gcsConfig.getString("qaEmail")
    val appsDomain = gcsConfig.getString("appsDomain")
  }

  object Projects {
    val default = gcsConfig.getString("serviceProject")
    val common = default
    val billingAccount = gcsConfig.getString("billingAccount")
    val billingAccountId = gcsConfig.getString("billingAccountId")
  }

  object Users {
    val notSoSecretPassword = users.getString("notSoSecretPassword")

    val dumbledore = Credentials(users.getString("dumbledore"), notSoSecretPassword)
    val voldemort = Credentials(users.getString("voldemort"), notSoSecretPassword)
    val admin = dumbledore

    val hermione = Credentials(users.getString("hermione"), notSoSecretPassword)
    val owner = hermione

    val mcgonagall = Credentials(users.getString("mcgonagall"), notSoSecretPassword)
    val snape = Credentials(users.getString("snape"), notSoSecretPassword)
    val curator = mcgonagall

    val harry = Credentials(users.getString("harry"), notSoSecretPassword)
    val ron = Credentials(users.getString("ron"), notSoSecretPassword)
    val draco = Credentials(users.getString("draco"), notSoSecretPassword)

    val fred = Credentials(users.getString("fred"), notSoSecretPassword)
    val george = Credentials(users.getString("george"), notSoSecretPassword)
    val bill = Credentials(users.getString("bill"), notSoSecretPassword)

    val lunaTemp = Credentials(users.getString("luna"), notSoSecretPassword)
    val lunaTempSubjectId = users.getString("lunaSubjectId")
    val nevilleTemp = Credentials(users.getString("neville"), notSoSecretPassword)
    val testUser = harry
    val dominique = harry
    val elvin = fred
  }

  object ChromeSettings {
    val chromedriverHost = chromeSettings.getString("chromedriverHost")
    val chromDriverPath = chromeSettings.getString("chromedriverPath")
  }
}
