import sbt._

object Dependencies {
  val scalaV = "2.13"

  val akkaV = "2.6.20"
  val akkaHttpV = "10.2.10"
  val googleV = "1.23.0"
  val automationGoogleV = "1.30.5"
  val scalaLoggingV = "3.9.5"
  val scalaTestV = "3.2.16"
  val http4sVersion = "1.0.0-M38"
  val slickV = "3.4.1"
  val guavaV = "31.1-jre"
  val monocleV = "2.1.0"
  val opencensusV = "0.29.0"
  val munitCatsEffectV = "1.0.7"
  val pact4sV = "0.9.0"

  private val workbenchLibsHash = "d764a9b"
  val serviceTestV = s"3.1-$workbenchLibsHash"
  val workbenchModelV = s"0.18-$workbenchLibsHash"

  // TODO update to 0.26 - DataprocInterpreter relies on deprecated class MemberType
  val workbenchGoogleV = s"0.23-4b46aac"
  val workbenchGoogle2V = s"0.30-$workbenchLibsHash"
  val workbenchOpenTelemetryV = s"0.5-$workbenchLibsHash"
  val workbenchOauth2V = s"0.4-$workbenchLibsHash"
  val workbenchAzureV = s"0.4-$workbenchLibsHash"

  val helmScalaSdkV = "0.0.8"

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
  val excludeBouncyCastle = ExclusionRule(organization = "org.bouncycastle", name = s"bcprov-jdk15on")
  val excludeBouncyCastleExt = ExclusionRule(organization = "org.bouncycastle", name = s"bcprov-ext-jdk15on")
  val excludeBouncyCastleUtil = ExclusionRule(organization = "org.bouncycastle", name = s"bcutil-jdk15on")
  val excludeBouncyCastlePkix = ExclusionRule(organization = "org.bouncycastle", name = s"bcpkix-jdk15on")
  val excludeSundrCodegen = ExclusionRule(organization = "io.sundr", name = s"sundr-codegen")
  val excludeStatsD = ExclusionRule(organization = "com.readytalk", name = s"metrics3-statsd")
  val excludeKms = ExclusionRule(organization = "com.google.cloud", name = s"google-cloud-kms")
  val excludeBigQuery = ExclusionRule(organization = "com.google.cloud", name = "google-cloud-bigquery")
  val excludeCloudBilling = ExclusionRule(organization = "com.google.cloud", name = "google-cloud-billing")

  val jose4j: ModuleID =  "org.bitbucket.b_c" % "jose4j" % "0.9.3"

  val logbackClassic: ModuleID =  "ch.qos.logback"              % "logback-classic" % "1.4.11"
  val scalaLogging: ModuleID =    "com.typesafe.scala-logging"  %% "scala-logging"  % scalaLoggingV
  val ficus: ModuleID =           "com.iheart"                  %% "ficus"          % "1.5.2"
  val enumeratum: ModuleID =      "com.beachape"                %% "enumeratum"     % "1.7.0"

  val akkaSlf4j: ModuleID =         "com.typesafe.akka" %% "akka-slf4j"           % akkaV
  val akkaHttp: ModuleID =          "com.typesafe.akka" %% "akka-http"            % akkaHttpV
  val akkaHttpSprayJson: ModuleID = "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpV
  val akkaStream: ModuleID = "com.typesafe.akka" %% "akka-stream" % akkaV
  val akkaTestKit: ModuleID =       "com.typesafe.akka" %% "akka-testkit"         % akkaV     % "test"
  val akkaHttpTestKit: ModuleID =   "com.typesafe.akka" %% "akka-http-testkit"    % akkaHttpV % "test"

  val googleRpc: ModuleID =                 "io.grpc"         % "grpc-core"                       % "1.55.1" excludeAll (excludeGuava, excludeGson, excludeFindbugsJsr)

  val scalaTest: ModuleID = "org.scalatest" %% "scalatest" % scalaTestV  % Test
  val scalaTestScalaCheck = "org.scalatestplus" %% "scalacheck-1-17" % s"${scalaTestV}.0" % Test // https://github.com/scalatest/scalatestplus-scalacheck
  val scalaTestMockito = "org.scalatestplus" %% "mockito-4-5" % "3.2.12.0" % Test // https://github.com/scalatest/scalatestplus-mockito
  val scalaTestSelenium =  "org.scalatestplus" %% "selenium-4-1" % "3.2.10.0" % Test // https://github.com/scalatest/scalatestplus-selenium

  // Exclude workbench-libs transitive dependencies so we can control the library versions individually.
  // workbench-google pulls in workbench-{util, model, metrics} and workbcan ench-metrics pulls in workbench-util.
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
    excludeGuava,
    excludeBouncyCastle,
    excludeBouncyCastleExt,
    excludeBouncyCastleUtil,
    excludeBouncyCastlePkix)
  val workbenchOpenTelemetryTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-opentelemetry" % workbenchOpenTelemetryV % Test classifier "tests" excludeAll (excludeGuava)

  val helmScalaSdk: ModuleID = "org.broadinstitute.dsp" %% "helm-scala-sdk" % helmScalaSdkV
  val helmScalaSdkTest: ModuleID = "org.broadinstitute.dsp" %% "helm-scala-sdk" % helmScalaSdkV % Test classifier "tests"

  val slick: ModuleID =           "com.typesafe.slick"  %% "slick"                % slickV excludeAll (excludeTypesafeConfig, excludeReactiveStream)
  val hikariCP: ModuleID =        "com.typesafe.slick"  %% "slick-hikaricp"       % slickV excludeAll (excludeSlf4j)
  val mysql: ModuleID =           "mysql"               % "mysql-connector-java"  % "8.0.32"
  val liquibase: ModuleID =       "org.liquibase"       % "liquibase-core"        % "4.20.0"
  val sealerate: ModuleID =       "ca.mrvisser"         %% "sealerate"            % "0.0.6"
  val googleCloudNio: ModuleID =  "com.google.cloud"    % "google-cloud-nio"      % "0.126.15" % Test // brought in for FakeStorageInterpreter

  // TODO [IA-4419] bump to non-RC version when 0.15.0 releases
  val circeYaml =         "io.circe"          %% "circe-yaml"           % "0.15.0-RC1"
  val http4sBlazeServer = "org.http4s"        %% "http4s-blaze-server"  % http4sVersion
  val http4sPrometheus = "org.http4s" %% "http4s-prometheus-metrics" % http4sVersion
  val http4sDsl =         "org.http4s"        %% "http4s-dsl"           % http4sVersion
  val http4sEmberClient = "org.http4s"        %% "http4s-ember-client"  % http4sVersion
  val http4sEmberServer = "org.http4s"        %% "http4s-ember-server"  % http4sVersion
  val http4sCirce       = "org.http4s"        %% "http4s-circe"  % http4sVersion
  val guava: ModuleID =   "com.google.guava"  % "guava"                 % guavaV
  val pact4sScalaTest =   "io.github.jbwheatley"  %% "pact4s-scalatest" % pact4sV % Test
  val pact4sCirce =       "io.github.jbwheatley"  %% "pact4s-circe"     % pact4sV
  val okHttp =            "com.squareup.okhttp3"  % "okhttp"            % "4.11.0"

  val workSpaceManagerV = "0.254.824-SNAPSHOT"

  def excludeJakartaActivationApi = ExclusionRule("jakarta.activation", "jakarta.activation-api")
  def excludeJakartaXmlBindApi = ExclusionRule("jakarta.xml.bind", "jakarta.xml.bind-api")
  def excludeJakarta(m: ModuleID): ModuleID = m.excludeAll(excludeJakartaActivationApi, excludeJakartaXmlBindApi)

  val workspaceManager = excludeJakarta("bio.terra" % "workspace-manager-client" % workSpaceManagerV)


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
    "net.logstash.logback" % "logstash-logback-encoder" % "7.3", // for structured logging in logback
    "com.github.julien-truffaut" %%  "monocle-core"  % monocleV,
    "com.github.julien-truffaut" %%  "monocle-macro" % monocleV,
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
    workspaceManager
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
    scalaTestMockito
  )

  val workbenchServiceTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-service-test" % serviceTestV % "test" classifier "tests" excludeAll (excludeGuava, excludeStatsD)
  val leonardoClient: ModuleID =  "org.broadinstitute.dsde.workbench" %% "leonardo-client" % "1.3.6-563edbd-SNAP"//"1.3.6-9d5d754"

  val automationDependencies = List(
    "com.fasterxml.jackson.module" %% "jackson-module-scala"   % "2.15.0" % "test",
    logbackClassic % "test",
    leonardoClient,
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
//    wsmClient
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
