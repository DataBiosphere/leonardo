import sbt._

object Dependencies {
  val scalaV = "2.12"

  val akkaV = "2.6.5"
  val akkaHttpV = "10.1.12"
  val jacksonV = "2.9.9"
  val jacksonDatabindV = "2.9.9.2" // jackson-databind has a security patch on the 2.9.9 branch
  val googleV = "1.23.0"
  val automationGoogleV = "1.30.5"
  val scalaLoggingV = "3.9.0"
  val scalaTestV = "3.1.2"
  val slickV = "3.3.2"
  val http4sVersion = "0.21.0" //remove http4s related dependencies once workbench-libs are upgraded
  val guavaV = "28.2-jre"
  val monocleV = "2.0.0"
  val opencensusV = "0.26.0"
  val serviceTestV = "0.18-d57f8a6"

  val workbenchUtilV = "0.5-4c7acd5"
  val workbenchModelV = "0.13-31cacc4"
  val workbenchGoogleV = "0.21-2a218f3"
  val workbenchGoogle2V = "0.10-b6673cf2-SNAP"
  val workbenchMetricsV = "0.3-c5b80d2"
  val workbenchOpenTelemetryV = "0.1-e66171c"

  val excludeAkkaHttp = ExclusionRule(organization = "com.typesafe.akka", name = "akka-http_2.12")
  val excludeAkkaStream = ExclusionRule(organization = "com.typesafe.akka", name = "akka-stream_2.12")
  val excludeAkkaHttpSprayJson = ExclusionRule(organization = "com.typesafe.akka", name = "akka-http-spray-json_2.12")
  val excludeGuavaJDK5 = ExclusionRule(organization = "com.google.guava", name = "guava-jdk5")
  val excludeGuava = ExclusionRule(organization = "com.google.guava", name = "guava")
  val excludeWorkbenchUtil = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-util_2.12")
  val excludeWorkbenchModel = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-model_2.12")
  val excludeWorkbenchMetrics = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-metrics_2.12")
  val excludeIoGrpc = ExclusionRule(organization = "io.grpc", name = "grpc-core")
  val excludeFindbugsJsr = ExclusionRule(organization = "com.google.code.findbugs", name = "jsr305")
  val excludeGson = ExclusionRule(organization = "com.google.code.gson", name = "gson")
  val excludeGoogleApiClient = ExclusionRule(organization = "com.google.api-client", name = "google-api-client")
  val excludeGoogleApiClientJackson2 = ExclusionRule(organization = "com.google.http-client", name = "google-http-client-jackson2")
  val excludeGoogleHttpClient = ExclusionRule(organization = "com.google.http-client", name = "google-http-client")
  val excludeJacksonCore = ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-core")
  val excludeJacksonAnnotation = ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-annotations")
  val excludeSlf4j = ExclusionRule(organization = "org.slf4j", name = "slf4j-api")
  val excludeLogbackCore = ExclusionRule(organization = "ch.qos.logback", name = "logback-core")
  val excludeLogbackClassic = ExclusionRule(organization = "ch.qos.logback", name = "logback-classic")
  val excludeTypesafeConfig = ExclusionRule(organization = "com.typesafe", name = "config")
  val excludeTypesafeSslConfig = ExclusionRule(organization = "com.typesafe", name = "ssl-config-core")
  val excludeGoogleError = ExclusionRule(organization = "com.google.errorprone", name = "error_prone_annotations")
  val excludeHttpComponent = ExclusionRule(organization = "org.apache.httpcomponents", name = "httpclient")
  val excludeReactiveStream = ExclusionRule(organization = "org.reactivestreams", name = "reactive-streams")
  val excludeAutoValue = ExclusionRule(organization = "com.google.auto.value", name = "auto-value_" + scalaV)
  val excludeAutoValueAnnotation = ExclusionRule(organization = "com.google.auto.value", name = "auto-value_annotations_" + scalaV)
  val excludeFirestore = ExclusionRule(organization = "com.google.cloud", name = s"google-cloud-firestore")
  val excludeBouncyCastle = ExclusionRule(organization = "org.bouncycastle", name = s"bcprov-jdk15on")
  val excludeBouncyCastleExt = ExclusionRule(organization = "org.bouncycastle", name = s"bcprov-ext-jdk15on")
  val excludeSundrCodegen = ExclusionRule(organization = "io.sundr", name = s"sundr-codegen")

  val jacksonAnnotations: ModuleID =  "com.fasterxml.jackson.core" % "jackson-annotations"  % jacksonV
  val jacksonDatabind: ModuleID =     "com.fasterxml.jackson.core" % "jackson-databind"     % jacksonDatabindV excludeAll (excludeJacksonAnnotation)
  val jacksonCore: ModuleID =         "com.fasterxml.jackson.core" % "jackson-core"         % jacksonV

  val logbackClassic: ModuleID =  "ch.qos.logback"              % "logback-classic" % "1.2.3"
  val ravenLogback: ModuleID =    "com.getsentry.raven"         % "raven-logback"   % "8.0.3" excludeAll (excludeJacksonCore, excludeSlf4j, excludeLogbackCore, excludeLogbackClassic)
  val scalaLogging: ModuleID =    "com.typesafe.scala-logging"  %% "scala-logging"  % scalaLoggingV
  val swaggerUi: ModuleID =       "org.webjars"                 % "swagger-ui"      % "3.25.0"
  val ficus: ModuleID =           "com.iheart"                  %% "ficus"          % "1.4.7"
  val httpClient: ModuleID =      "org.apache.httpcomponents"   % "httpclient"      % "4.5.5" // upgrading a transitive dependency to avoid security warnings
  val enumeratum: ModuleID =      "com.beachape"                %% "enumeratum"     % "1.5.13"

  val akkaSlf4j: ModuleID =         "com.typesafe.akka" %% "akka-slf4j"           % akkaV
  val akkaHttp: ModuleID =          "com.typesafe.akka" %% "akka-http"            % akkaHttpV
  val akkaHttpSprayJson: ModuleID = "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpV
  val akkaStream: ModuleID = "com.typesafe.akka" %% "akka-stream" % akkaV
  val akkaTestKit: ModuleID =       "com.typesafe.akka" %% "akka-testkit"         % akkaV     % "test"
  val akkaHttpTestKit: ModuleID =   "com.typesafe.akka" %% "akka-http-testkit"    % akkaHttpV % "test"

  val googleDataproc: ModuleID =            "com.google.apis" % "google-api-services-dataproc"    % s"v1-rev91-$googleV" excludeAll (excludeGuavaJDK5, excludeJacksonCore, excludeFindbugsJsr, excludeHttpComponent, excludeFirestore)
  val googleRpc: ModuleID =                 "io.grpc"         % "grpc-core"                       % "1.28.0" excludeAll (excludeGuava, excludeGson, excludeFindbugsJsr, excludeAutoValueAnnotation, excludeAutoValue)
  val googleOAuth2: ModuleID =              "com.google.auth" % "google-auth-library-oauth2-http" % "0.9.1" excludeAll (excludeGuava, excludeFindbugsJsr, excludeGoogleApiClient, excludeGoogleApiClientJackson2, excludeGoogleHttpClient, excludeHttpComponent)

  val scalaTest: ModuleID = "org.scalatest" %% "scalatest"    % scalaTestV  % "test"
  val mockito: ModuleID =   "org.mockito"   % "mockito-core"  % "3.2.4"    % "test"
  val scalaTestScalaCheck = "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2" % Test //Since scalatest 3.1.0, scalacheck support is moved to `scalatestplus`
  val scalaTestMockito = "org.scalatestplus" %% "scalatestplus-mockito" % "1.0.0-M2" % Test //Since scalatest 3.1.0, mockito support is moved to `scalatestplus`
  val scalaTestSelenium =  "org.scalatestplus" %% "scalatestplus-selenium" % "1.0.0-M2" % Test //Since scalatest 3.1.0, selenium support is moved to `scalatestplus`

  // Exclude workbench-libs transitive dependencies so we can control the library versions individually.
  // workbench-google pulls in workbench-{util, model, metrics} and workbcan ench-metrics pulls in workbench-util.
  val workbenchUtil: ModuleID =         "org.broadinstitute.dsde.workbench" %% "workbench-util"     % workbenchUtilV excludeAll (excludeWorkbenchModel, excludeGoogleError, excludeGuava)
  val workbenchModel: ModuleID =        "org.broadinstitute.dsde.workbench" %% "workbench-model"    % workbenchModelV excludeAll (excludeGoogleError, excludeGuava)
  val workbenchGoogle: ModuleID =       "org.broadinstitute.dsde.workbench" %% "workbench-google"   % workbenchGoogleV excludeAll (excludeWorkbenchUtil, excludeWorkbenchModel, excludeIoGrpc, excludeFindbugsJsr, excludeGoogleApiClient, excludeGoogleError, excludeHttpComponent, excludeAutoValue, excludeAutoValueAnnotation, excludeGuava)
  val workbenchGoogle2: ModuleID =      "org.broadinstitute.dsde.workbench" %% "workbench-google2"  % workbenchGoogle2V excludeAll (excludeWorkbenchUtil,
    excludeWorkbenchModel,
    excludeWorkbenchMetrics,
    excludeIoGrpc,
    excludeFindbugsJsr,
    excludeGoogleApiClient,
    excludeGoogleError,
    excludeHttpComponent,
    excludeAutoValue,
    excludeAutoValueAnnotation,
    excludeFirestore,
    excludeBouncyCastle,
    excludeBouncyCastleExt,
    excludeSundrCodegen,
    excludeGuava)
  val workbenchGoogleTest: ModuleID =   "org.broadinstitute.dsde.workbench" %% "workbench-google"   % workbenchGoogleV  % "test" classifier "tests" excludeAll (excludeWorkbenchUtil, excludeWorkbenchModel, excludeGuava)
  val workbenchGoogle2Test: ModuleID =  "org.broadinstitute.dsde.workbench" %% "workbench-google2"  % workbenchGoogle2V % "test" classifier "tests" excludeAll (excludeGuava) //for generators
  val workbenchOpenTelemetry: ModuleID =     "org.broadinstitute.dsde.workbench" %% "workbench-opentelemetry" % workbenchOpenTelemetryV excludeAll (excludeGuava)
  val workbenchOpenTelemetryTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-opentelemetry" % workbenchOpenTelemetryV % "test" classifier "tests" excludeAll (excludeGuava)

  val slick: ModuleID =           "com.typesafe.slick"  %% "slick"                % slickV excludeAll (excludeTypesafeConfig, excludeReactiveStream)
  val hikariCP: ModuleID =        "com.typesafe.slick"  %% "slick-hikaricp"       % slickV excludeAll (excludeSlf4j)
  val mysql: ModuleID =           "mysql"               % "mysql-connector-java"  % "8.0.18"
  val liquibase: ModuleID =       "org.liquibase"       % "liquibase-core"        % "3.8.8"
  val sealerate: ModuleID =       "ca.mrvisser"         %% "sealerate"            % "0.0.5"
  val googleCloudNio: ModuleID =  "com.google.cloud"    % "google-cloud-nio"      % "0.107.0-alpha" % "test" // brought in for FakeStorageInterpreter

  val http4sCirce =       "org.http4s"        %% "http4s-circe"         % http4sVersion
  val http4sBlazeClient = "org.http4s"        %% "http4s-blaze-client"  % http4sVersion
  val http4sDsl =         "org.http4s"        %% "http4s-dsl"           % http4sVersion
  val fs2Io: ModuleID =   "co.fs2"            %% "fs2-io"               % "2.0.1"
  val guava: ModuleID =   "com.google.guava"  % "guava"                 % guavaV

  val coreDependencies = List(
    scalaTest,
    slick,
    guava,
    workbenchModel,
    workbenchGoogle2,
    workbenchGoogle2Test,
    workbenchOpenTelemetry,
    workbenchOpenTelemetryTest,
    "net.logstash.logback" % "logstash-logback-encoder" % "6.2", // for structured logging in logback
    "org.scalacheck" %% "scalacheck" % "1.14.1" % "test",
    sealerate,
    enumeratum,
    http4sCirce,
    http4sBlazeClient,
    http4sDsl,
    scalaTestScalaCheck
  )

  val rootDependencies = Seq(
    // proactively pull in latest versions of Jackson libs, instead of relying on the versions
    // specified as transitive dependencies, due to OWASP DependencyCheck warnings for earlier versions.
    jacksonAnnotations,
    jacksonDatabind,
    jacksonCore,
    fs2Io,
    logbackClassic,
    ravenLogback,
    scalaLogging,
    swaggerUi,
    ficus,
    enumeratum,
    akkaSlf4j,
    akkaHttp,
    akkaHttpSprayJson,
    akkaTestKit,
    akkaHttpTestKit,
    akkaStream,
    "de.heikoseeberger" %% "akka-http-circe" % "1.32.0" excludeAll(excludeAkkaHttp, excludeAkkaStream),
    googleDataproc,
    googleRpc,
    googleOAuth2,
    mockito,
    hikariCP,
    workbenchUtil,
    workbenchGoogle,
    workbenchGoogleTest,
    "org.typelevel" %% "cats-mtl-core"  % "0.7.0",
    "org.typelevel" %% "cats-effect"    % "2.0.0", //forcing cats 2.0.0
    "com.rms.miu" %% "slick-cats" % "0.10.1",
    googleCloudNio,
    "com.github.julien-truffaut" %%  "monocle-core"  % monocleV,
    "com.github.julien-truffaut" %%  "monocle-macro" % monocleV,
    mysql,
    liquibase,
    "de.heikoseeberger" %% "akka-http-circe" % "1.31.0",
    "com.github.sebruck" %% "opencensus-scala-akka-http" % "0.7.2",

    // Dependent on the trace exporters you want to use add one or more of the following
    "io.opencensus" % "opencensus-exporter-trace-stackdriver" % opencensusV,
    "org.http4s" %% "http4s-blaze-server" % http4sVersion % Test,
    scalaTestSelenium,
    scalaTestMockito
  )

  val excludeGuavaJdk5 = ExclusionRule(organization = "com.google.guava", name = "guava-jdk5")
  val excludeApacheHttpClient = ExclusionRule(organization = "org.apache.httpcomponents", name = "httpclient")
  val excludeGoogleJsr305 = ExclusionRule(organization = "com.google.code.findbugs", name = "jsr305")
  val excludeWorkbenchGoogle = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-google_" + scalaV)

  val workbenchServiceTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-service-test" % serviceTestV % "test" classifier "tests" excludeAll (excludeWorkbenchModel, excludeWorkbenchGoogle, excludeGuava, excludeAkkaHttpSprayJson)

  val automationDependencies = List(
    // proactively pull in latest versions of Jackson libs, instead of relying on the versions
    // specified as transitive dependencies, due to OWASP DependencyCheck warnings for earlier versions.
    "com.fasterxml.jackson.core" % "jackson-annotations"  % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-databind"     % jacksonV excludeAll (excludeJacksonAnnotation),
    "com.fasterxml.jackson.core" % "jackson-core"         % jacksonV,
    "com.fasterxml.jackson.module" % ("jackson-module-scala_" + scalaV) % jacksonV,
    "ch.qos.logback" % "logback-classic" % "1.2.3" % "test",
    "com.google.apis" % "google-api-services-oauth2" % "v1-rev142-1.23.0" excludeAll (excludeGuavaJdk5, excludeGuava, excludeApacheHttpClient, excludeGoogleJsr305, excludeJacksonCore),
    "com.google.api-client" % "google-api-client" % automationGoogleV excludeAll (excludeGuavaJdk5, excludeGuava, excludeApacheHttpClient, excludeGoogleJsr305, excludeJacksonCore),
    "com.google.auto.value" % "auto-value" % "1.6.6",
    "com.google.auto.value" % "auto-value-annotations" % "1.6.6",

    "com.typesafe.akka" %% "akka-http-core" % akkaHttpV,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % "test",
    "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    "com.typesafe.akka" %% "akka-testkit" % akkaV % "test",
    "com.typesafe.akka" %% "akka-slf4j" % akkaV,
    scalaTest,
    "org.seleniumhq.selenium" % "selenium-java" % "4.0.0-alpha-6" % "test",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0",
    "org.apache.commons" % "commons-text" % "1.2",
    googleRpc,
    workbenchUtil,
    workbenchModel,
    workbenchGoogle,
    workbenchGoogle2,
    workbenchServiceTest,
    googleCloudNio,
    akkaHttpSprayJson,
    scalaTestSelenium,
    scalaTestMockito,
    // required by workbenchGoogle
    "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.6" % "provided"
  )
}
