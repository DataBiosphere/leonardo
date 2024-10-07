import sbt._

object Dependencies {
  val scalaV = "2.13"

  val akkaV = "2.6.20"
  val akkaHttpV = "10.2.10"
  val googleV = "1.23.0"
  val automationGoogleV = "1.30.5"
  val scalaLoggingV = "3.9.5"
  val scalaTestV = "3.2.17"
  val http4sVersion = "1.0.0-M38" //Do not upgrade to M40 until workbench-libs does (missing M40 for blaze as of 08/2023
  val slickV = "3.4.1"
  val guavaV = "32.1.3-jre"
  val monocleV = "3.2.0"
  val opencensusV = "0.29.0"
  val munitCatsEffectV = "1.0.7"
  val pact4sV = "0.10.0"

  private val workbenchLibsHash = "9efd95c-SNAP"
  val serviceTestV = s"5.0-$workbenchLibsHash"
  val workbenchModelV = s"0.20-$workbenchLibsHash"
  val workbenchGoogleV = s"0.32-$workbenchLibsHash"
  val workbenchGoogle2V = s"0.36-$workbenchLibsHash"
  val workbenchOpenTelemetryV = s"0.8-$workbenchLibsHash"
  val workbenchOauth2V = s"0.7-$workbenchLibsHash"
  val workbenchAzureV = s"0.8-$workbenchLibsHash"

  val helmScalaSdkV = "0.0.8.5"

  val excludeAkkaHttp = ExclusionRule(organization = "com.typesafe.akka", name = s"akka-http_${scalaV}")
  val excludeAkkaStream = ExclusionRule(organization = "com.typesafe.akka", name = s"akka-stream_${scalaV}")
  val excludeAkkaHttpSprayJson = ExclusionRule(organization = "com.typesafe.akka", name = s"akka-http-spray-json_${scalaV}")
  val excludeGuavaJDK5 = ExclusionRule(organization = "com.google.guava", name = "guava-jdk5")
  val excludeGuava = ExclusionRule(organization = "com.google.guava", name = "guava")
  val excludeWorkbenchMetrics = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = s"workbench-metrics_${scalaV}")
  val excludeIoGrpc = ExclusionRule(organization = "io.grpc", name = "grpc-core")
  val excludeFindbugsJsr = ExclusionRule(organization = "com.google.code.findbugs", name = "jsr305")
  val excludeGson = ExclusionRule(organization = "com.google.code.gson", name = "gson")
  val excludeGoogleApiClient = ExclusionRule(organization = "com.google.api-client", name = "google-api-client")
  val excludeGoogleHttpClient = ExclusionRule(organization = "com.google.http-client", name = "google-http-client")
  val excludeJacksonCore = ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-core")
  val excludeJacksonAnnotation = ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-annotations")
  val excludeSlf4j = ExclusionRule(organization = "org.slf4j", name = "slf4j-api")
  val excludeTypesafeConfig = ExclusionRule(organization = "com.typesafe", name = "config")
  val excludeTypesafeSslConfig = ExclusionRule(organization = "com.typesafe", name = "ssl-config-core")
  val excludeGoogleError = ExclusionRule(organization = "com.google.errorprone", name = "error_prone_annotations")
  val excludeHttpComponent = ExclusionRule(organization = "org.apache.httpcomponents", name = "httpclient")
  val excludeReactiveStream = ExclusionRule(organization = "org.reactivestreams", name = "reactive-streams")
  val excludeFirestore = ExclusionRule(organization = "com.google.cloud", name = s"google-cloud-firestore")
  val excludeSundrCodegen = ExclusionRule(organization = "io.sundr", name = s"sundr-codegen")
  val excludeStatsD = ExclusionRule(organization = "com.readytalk", name = s"metrics3-statsd")
  val excludeKms = ExclusionRule(organization = "com.google.cloud", name = s"google-cloud-kms")
  val excludeBigQuery = ExclusionRule(organization = "com.google.cloud", name = "google-cloud-bigquery")
  val excludeCloudBilling = ExclusionRule(organization = "com.google.cloud", name = "google-cloud-billing")

  val jose4j: ModuleID =  "org.bitbucket.b_c" % "jose4j" % "0.9.4"

  val logbackClassic: ModuleID =  "ch.qos.logback"              % "logback-classic" % "1.4.14"
  val scalaLogging: ModuleID =    "com.typesafe.scala-logging"  %% "scala-logging"  % scalaLoggingV
  val ficus: ModuleID =           "com.iheart"                  %% "ficus"          % "1.5.2"
  val enumeratum: ModuleID =      "com.beachape"                %% "enumeratum"     % "1.7.3"

  val akkaSlf4j: ModuleID =         "com.typesafe.akka" %% "akka-slf4j"           % akkaV
  val akkaHttp: ModuleID =          "com.typesafe.akka" %% "akka-http"            % akkaHttpV
  val akkaHttpSprayJson: ModuleID = "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpV
  val akkaStream: ModuleID = "com.typesafe.akka" %% "akka-stream" % akkaV
  val akkaTestKit: ModuleID =       "com.typesafe.akka" %% "akka-testkit"         % akkaV     % "test"
  val akkaHttpTestKit: ModuleID =   "com.typesafe.akka" %% "akka-http-testkit"    % akkaHttpV % "test"

  val googleRpc: ModuleID =                 "io.grpc"         % "grpc-core"                       % "1.58.0" excludeAll (excludeGuava, excludeGson, excludeFindbugsJsr)

  val scalaTest: ModuleID = "org.scalatest" %% "scalatest" % scalaTestV  % Test
  val scalaTestScalaCheck = "org.scalatestplus" %% "scalacheck-1-17" % s"${scalaTestV}.0" % Test // https://github.com/scalatest/scalatestplus-scalacheck
  val scalaTestMockito = "org.scalatestplus" %% "mockito-4-5" % "3.2.12.0" % Test // https://github.com/scalatest/scalatestplus-mockito
  val scalaTestSelenium =  "org.scalatestplus" %% "selenium-4-1" % "3.2.12.1" % Test // https://github.com/scalatest/scalatestplus-selenium

  // Exclude workbench-libs transitive dependencies so we can control the library versions individually.
  // workbench-google pulls in workbench-{util, model, metrics} and workbench-metrics pulls in workbench-util.
  val workbenchModel: ModuleID =        "org.broadinstitute.dsde.workbench" %% "workbench-model"    % workbenchModelV excludeAll (excludeGoogleError, excludeGuava)
  val workbenchGoogle: ModuleID =       "org.broadinstitute.dsde.workbench" %% "workbench-google"   % workbenchGoogleV excludeAll (
    excludeIoGrpc,
    excludeFindbugsJsr,
    excludeGoogleApiClient,
    excludeGoogleError,
    excludeHttpComponent,
    excludeGuava,
    excludeStatsD,
    excludeKms)
  val workbenchGoogle2: ModuleID =      "org.broadinstitute.dsde.workbench" %% "workbench-google2"  % workbenchGoogle2V excludeAll (
    excludeWorkbenchMetrics,
    excludeIoGrpc,
    excludeFindbugsJsr,
    excludeGoogleError,
    excludeHttpComponent,
    excludeFirestore,
    excludeKms,
    excludeBigQuery,
    excludeCloudBilling,
    excludeSundrCodegen,
    excludeGuava
  )
  val workbenchAzure: ModuleID =      "org.broadinstitute.dsde.workbench" %% "workbench-azure"  % workbenchAzureV
  val workbenchOauth2: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-oauth2" % workbenchOauth2V
  val workbenchOauth2Tests: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-oauth2" % workbenchOauth2V % "test" classifier "tests"
  val workbenchGoogleTest: ModuleID =   "org.broadinstitute.dsde.workbench" %% "workbench-google"   % workbenchGoogleV  % "test" classifier "tests" excludeAll (excludeGuava, excludeStatsD)
  val workbenchGoogle2Test: ModuleID =  "org.broadinstitute.dsde.workbench" %% "workbench-google2"  % workbenchGoogle2V % "test" classifier "tests" excludeAll (excludeGuava) //for generators
  val workbenchAzureTest: ModuleID =  "org.broadinstitute.dsde.workbench" %% "workbench-azure"  % workbenchAzureV % "test" classifier "tests"
  val workbenchOpenTelemetry: ModuleID =     "org.broadinstitute.dsde.workbench" %% "workbench-opentelemetry" % workbenchOpenTelemetryV excludeAll (
    excludeIoGrpc,
    excludeGuava)
  val workbenchOpenTelemetryTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-opentelemetry" % workbenchOpenTelemetryV % Test classifier "tests" excludeAll (excludeGuava)

  val helmScalaSdk: ModuleID = "org.broadinstitute.dsp" %% "helm-scala-sdk" % helmScalaSdkV
  val helmScalaSdkTest: ModuleID = "org.broadinstitute.dsp" %% "helm-scala-sdk" % helmScalaSdkV % Test classifier "tests"

  val slick: ModuleID =           "com.typesafe.slick"  %% "slick"                % slickV excludeAll (excludeTypesafeConfig, excludeReactiveStream)
  val hikariCP: ModuleID =        "com.typesafe.slick"  %% "slick-hikaricp"       % slickV excludeAll (excludeSlf4j)
  val mysql: ModuleID =           "mysql"               % "mysql-connector-java"  % "8.0.32"
  val liquibase: ModuleID =       "org.liquibase"       % "liquibase-core"        % "4.20.0"
  val sealerate: ModuleID =       "ca.mrvisser"         %% "sealerate"            % "0.0.6"
  val googleCloudNio: ModuleID =  "com.google.cloud"    % "google-cloud-nio"      % "0.127.7" % Test // brought in for FakeStorageInterpreter

  val circeYaml =         "io.circe"          %% "circe-yaml"           % "0.15.1"
  val http4sBlazeServer = "org.http4s"        %% "http4s-blaze-server"  % http4sVersion
  val http4sPrometheus = "org.http4s" %% "http4s-prometheus-metrics" % http4sVersion
  val http4sDsl =         "org.http4s"        %% "http4s-dsl"           % http4sVersion
  val http4sEmberClient = "org.http4s"        %% "http4s-ember-client"  % http4sVersion
  val http4sEmberServer = "org.http4s"        %% "http4s-ember-server"  % http4sVersion
  val http4sCirce       = "org.http4s"        %% "http4s-circe"  % http4sVersion
  val guava: ModuleID =   "com.google.guava"  % "guava"                 % guavaV
  val pact4sScalaTest =   "io.github.jbwheatley"  %% "pact4s-scalatest" % pact4sV % Test
  val pact4sCirce =       "io.github.jbwheatley"  %% "pact4s-circe"     % pact4sV
  val okHttp =            "com.squareup.okhttp3"  % "okhttp"            % "4.12.0"

  val workSpaceManagerV = "0.254.1127-SNAPSHOT"
  val terraCommonLibV = "0.0.94-SNAPSHOT"
  val bpmV = "0.1.548-SNAPSHOT"

  def excludeJakartaActivationApi = ExclusionRule("jakarta.activation", "jakarta.activation-api")
  def excludeJakartaXmlBindApi = ExclusionRule("jakarta.xml.bind", "jakarta.xml.bind-api")
  def excludeJakarta(m: ModuleID): ModuleID = m.excludeAll(excludeJakartaActivationApi)
  def excludeSpringBoot = ExclusionRule("org.springframework.boot")
  def excludeSpringAop = ExclusionRule("org.springframework.spring-aop")
  def excludeSpringData = ExclusionRule("org.springframework.data")
  def excludeSpringFramework = ExclusionRule("org.springframework")
  def excludeOpenCensus = ExclusionRule("io.opencensus")
  def excludeGoogleFindBugs = ExclusionRule("com.google.code.findbugs")
  def excludeBroadWorkbench = ExclusionRule("org.broadinstitute.dsde.workbench")
  def excludePostgresql = ExclusionRule("org.postgresql", "postgresql")
  def excludeSnakeyaml = ExclusionRule("org.yaml", "snakeyaml")
  // [IA-4939] commons-text:1.9 is unsafe
  def excludeCommonsText = ExclusionRule("org.apache.commons", "commons-text")
  def tclExclusions(m: ModuleID): ModuleID = m.excludeAll(excludeSpringBoot, excludeSpringAop, excludeSpringData, excludeSpringFramework, excludeOpenCensus, excludeGoogleFindBugs, excludeBroadWorkbench, excludePostgresql, excludeSnakeyaml, excludeSlf4j, excludeCommonsText)
  val workspaceManager = excludeJakarta("bio.terra" % "workspace-manager-client" % workSpaceManagerV)
  val bpm = excludeJakarta("bio.terra" % "billing-profile-manager-client" % bpmV)
  val terraCommonLib = tclExclusions(excludeJakarta("bio.terra" % "terra-common-lib" % terraCommonLibV classifier "plain"))

  val coreDependencies = List(
    jose4j,
    workbenchOauth2,
    workbenchOauth2Tests,
    scalaTest,
    slick,
    guava,
    workbenchModel,
    workbenchGoogle2,
    workbenchGoogle2Test,
    workbenchOpenTelemetry,
    workbenchOpenTelemetryTest,
    helmScalaSdk,
    helmScalaSdkTest,
    "net.logstash.logback" % "logstash-logback-encoder" % "7.4", // for structured logging in logback
    "dev.optics" %%  "monocle-core"  % monocleV,
    "dev.optics" %%  "monocle-macro" % monocleV,
    // using provided because `http` depends on `core`, and `http`'s `opencensus-exporter-trace-stackdriver`
    // brings in an older version of `pureconfig`
    "com.github.pureconfig" %% "pureconfig" % "0.17.4" % Provided,
    sealerate,
    enumeratum,
    circeYaml,
    http4sDsl,
    scalaTestScalaCheck,
    workbenchAzure,
    workbenchAzureTest,
    logbackClassic,
    workspaceManager,
    bpm,
    terraCommonLib
  )

  val httpDependencies = Seq(
    scalaLogging,
    ficus,
    enumeratum,
    akkaSlf4j,
    akkaHttp,
    akkaHttpSprayJson,
    akkaTestKit,
    akkaHttpTestKit,
    akkaStream,
    http4sPrometheus,
    "de.heikoseeberger" %% "akka-http-circe" % "1.39.2" excludeAll(excludeAkkaHttp, excludeAkkaStream),
    googleRpc,

    hikariCP,
    workbenchGoogle,
    workbenchGoogleTest,
    googleCloudNio,
    mysql,
    liquibase,
    "com.github.sebruck" %% "opencensus-scala-akka-http" % "0.7.2",
    "com.auth0" % "java-jwt" % "4.4.0",
    http4sBlazeServer % Test,
    scalaTestSelenium,
    scalaTestMockito,
    "com.azure" % "azure-identity" % "1.10.4"
  )

  val workbenchServiceTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-service-test" % serviceTestV % "test" classifier "tests" excludeAll (excludeGuava, excludeStatsD)
  val leonardoClient: ModuleID =  "org.broadinstitute.dsde.workbench" %% "leonardo-client" % "1.3.6-35973f1-SNAP"
  // You should not be using SSH functionality outside of the tests according to security team's guidance
  val ssh: ModuleID = "com.hierynomus" % "sshj" % "0.37.0" % "test"
  val googleCloudOSLogin = "com.google.cloud" % "google-cloud-os-login" % "2.2.7" % "test"

  val automationOverrides = List(guava)

  val automationDependencies = List(
    "com.fasterxml.jackson.module" %% "jackson-module-scala"   % "2.17.1" % "test",
    logbackClassic % "test",
    leonardoClient,
    ssh,
    googleCloudOSLogin,
    "com.typesafe.akka" %% "akka-http-core" % akkaHttpV,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % "test",
    "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    "com.typesafe.akka" %% "akka-testkit" % akkaV % "test",
    "com.typesafe.akka" %% "akka-slf4j" % akkaV,
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingV,
    googleRpc,
    workbenchGoogle,
    workbenchGoogle2,
    workbenchServiceTest,
    googleCloudNio,
    akkaHttpSprayJson,
    scalaTest,
    scalaTestSelenium,
    scalaTestMockito,
    http4sBlazeServer % Test,
    okHttp % Test
  )

  val pact4sDependencies = Seq(
    pact4sScalaTest,
    pact4sCirce,
    http4sEmberClient,
    http4sDsl,
    http4sEmberServer,
    http4sCirce,
    scalaTest
  )

}
