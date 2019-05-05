import sbt._

object Dependencies {
  val akkaV         = "2.5.13"
  val akkaHttpV     = "10.1.1"
  val jacksonV      = "2.9.8"
  val googleV       = "1.23.0"
  val scalaLoggingV = "3.9.0"
  val scalaTestV    = "3.0.5"
  val slickV        = "3.2.3"

  val workbenchUtilV    = "0.5-4c7acd5"
  val workbenchModelV   = "0.11-2bddd5b"
  val workbenchGoogleV  = "0.18-6942040"
  val workbenchMetricsV = "0.3-c5b80d2"

  val samV =  "1.0-5cdffb4"

  val excludeAkkaActor          = ExclusionRule(organization = "com.typesafe.akka", name = "akka-actor_2.12")
  val excludeGuavaJDK5          = ExclusionRule(organization = "com.google.guava", name = "guava-jdk5")
  val excludeGuava              = ExclusionRule(organization = "com.google.guava", name = "guava")
  val excludeWorkbenchUtil      = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-util_2.12")
  val excludeWorkbenchModel     = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-model_2.12")
  val excludeWorkbenchMetrics   = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-metrics_2.12")
  val excludeIoGrpc             = ExclusionRule(organization = "io.grpc", name = "grpc-core")
  val excludeFindbugsJsr        = ExclusionRule(organization = "com.google.code.findbugs", name = "jsr305")
  val excludeGson               = ExclusionRule(organization = "com.google.code.gson", name = "gson")
  val excludeGoogleApiClient    = ExclusionRule(organization = "com.google.api-client", name = "google-api-client")
  val excludeGoogleApiClientJackson2  = ExclusionRule(organization = "com.google.http-client", name = "google-http-client-jackson2")
  val excludeGoogleHttpClient   = ExclusionRule(organization = "com.google.http-client", name = "google-http-client")
  val excludeJacksonCore        = ExclusionRule(organization = "com.fasterxml.jackson.core", name =  "jackson-core")
  val excludeJacksonAnnotation  = ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-annotations")
  val excludeSlf4j              = ExclusionRule(organization = "org.slf4j", name = "slf4j-api")
  val excludeLogbackCore        = ExclusionRule(organization = "ch.qos.logback", name = "logback-core")
  val excludeLogbackClassic = ExclusionRule(organization = "ch.qos.logback", name = "logback-classic")
  val excludeTypesafeConfig     = ExclusionRule(organization = "com.typesafe", name = "config")
  val excludeTypesafeSslConfig                = ExclusionRule(organization = "com.typesafe", name = "ssl-config-core")
  val excludeGoogleError = ExclusionRule(organization = "com.google.errorprone", name = "error_prone_annotations")
  val excludeHttpComponent      = ExclusionRule(organization = "org.apache.httpcomponents", name = "httpclient")
  val excludeReactiveStream     = ExclusionRule(organization = "org.reactivestreams", name = "reactive-streams")

  val jacksonAnnotations: ModuleID = "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonV
  val jacksonDatabind: ModuleID =    "com.fasterxml.jackson.core" % "jackson-databind"    % jacksonV excludeAll(excludeJacksonAnnotation)
  val jacksonCore: ModuleID =        "com.fasterxml.jackson.core" % "jackson-core"        % jacksonV

  val logbackClassic: ModuleID = "ch.qos.logback"             %  "logback-classic" % "1.2.3"  
  val ravenLogback: ModuleID =   "com.getsentry.raven"        %  "raven-logback"   % "8.0.3" excludeAll(excludeJacksonCore, excludeSlf4j, excludeLogbackCore, excludeLogbackClassic)
  val scalaLogging: ModuleID =   "com.typesafe.scala-logging" %% "scala-logging"   % scalaLoggingV
  val swaggerUi: ModuleID =      "org.webjars"                %  "swagger-ui"      % "2.2.5"
  val ficus: ModuleID =          "com.iheart"                 %% "ficus"           % "1.4.3"
  val httpClient: ModuleID =     "org.apache.httpcomponents"  % "httpclient"       % "4.5.5"  // upgrading a transitive dependency to avoid security warnings
  val enumeratum: ModuleID =     "com.beachape"               %% "enumeratum"      % "1.5.13"

  val akkaActor: ModuleID =         "com.typesafe.akka"   %%  "akka-actor"           % akkaV excludeAll(excludeTypesafeSslConfig)
  val akkaContrib: ModuleID =       "com.typesafe.akka"   %%  "akka-contrib"         % akkaV excludeAll(excludeTypesafeConfig)
  val akkaSlf4j: ModuleID =         "com.typesafe.akka"   %%  "akka-slf4j"           % akkaV
  val akkaHttp: ModuleID =          "com.typesafe.akka"   %%  "akka-http"            % akkaHttpV           excludeAll(excludeAkkaActor)
  val akkaHttpSprayJson: ModuleID = "com.typesafe.akka"   %%  "akka-http-spray-json" % akkaHttpV
  val akkaTestKit: ModuleID =       "com.typesafe.akka"   %%  "akka-testkit"         % akkaV     % "test"
  val akkaHttpTestKit: ModuleID =   "com.typesafe.akka"   %%  "akka-http-testkit"    % akkaHttpV % "test"

  val googleDataproc: ModuleID =    "com.google.apis"     % "google-api-services-dataproc" % s"v1-rev91-$googleV" excludeAll(excludeGuavaJDK5, excludeJacksonCore, excludeFindbugsJsr, excludeHttpComponent)
  val googleRpc: ModuleID = "io.grpc" % "grpc-core" % "1.12.0" excludeAll(excludeGuava, excludeGson, excludeFindbugsJsr)
  val googleOAuth2: ModuleID = "com.google.auth" % "google-auth-library-oauth2-http" % "0.9.1" excludeAll(excludeGuava, excludeFindbugsJsr, excludeGoogleApiClient, excludeGoogleApiClientJackson2, excludeGoogleHttpClient, excludeHttpComponent)
  val googleSourceRepositories: ModuleID = "com.google.apis" % "google-api-services-sourcerepo" % s"v1-rev21-$googleV" excludeAll(excludeGuavaJDK5)

  val scalaTest: ModuleID = "org.scalatest" %% "scalatest"    % scalaTestV % "test"
  val mockito: ModuleID =   "org.mockito"    % "mockito-core" % "2.18.3"   % "test"

  // Exclude workbench-libs transitive dependencies so we can control the library versions individually.
  // workbench-google pulls in workbench-{util, model, metrics} and workbench-metrics pulls in workbench-util.
  val workbenchUtil: ModuleID       = "org.broadinstitute.dsde.workbench" %% "workbench-util"    % workbenchUtilV   excludeAll(excludeWorkbenchModel, excludeGoogleError)
  val workbenchModel: ModuleID      = "org.broadinstitute.dsde.workbench" %% "workbench-model"   % workbenchModelV  excludeAll(excludeGoogleError)
  val workbenchGoogle: ModuleID     = "org.broadinstitute.dsde.workbench" %% "workbench-google"  % workbenchGoogleV excludeAll(excludeWorkbenchUtil, excludeWorkbenchModel, excludeWorkbenchMetrics, excludeIoGrpc, excludeFindbugsJsr, excludeGoogleApiClient, excludeGoogleError, excludeHttpComponent)
  val workbenchGoogleTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-google"  % workbenchGoogleV % "test" classifier "tests" excludeAll(excludeWorkbenchUtil, excludeWorkbenchModel)
  val workbenchMetrics: ModuleID    = "org.broadinstitute.dsde.workbench" %% "workbench-metrics" % workbenchMetricsV excludeAll(excludeWorkbenchUtil, excludeSlf4j)

  val sam: ModuleID = "org.broadinstitute.dsde.sam-client" %% "sam" % samV

  val slick: ModuleID =     "com.typesafe.slick" %% "slick"                 % slickV excludeAll(excludeTypesafeConfig, excludeReactiveStream)
  val hikariCP: ModuleID =  "com.typesafe.slick" %% "slick-hikaricp"        % slickV excludeAll(excludeSlf4j)
  val mysql: ModuleID =     "mysql"               % "mysql-connector-java"  % "8.0.11"
  val liquibase: ModuleID = "org.liquibase"       % "liquibase-core"        % "3.5.3"
  val sealerate: ModuleID = "ca.mrvisser" %% "sealerate" % "0.0.5"

  val rootDependencies = Seq(
    // proactively pull in latest versions of Jackson libs, instead of relying on the versions
    // specified as transitive dependencies, due to OWASP DependencyCheck warnings for earlier versions.
    jacksonAnnotations,
    jacksonDatabind,
    jacksonCore,

    logbackClassic,
    ravenLogback,
    scalaLogging,
    swaggerUi,
    ficus,
    httpClient,
    enumeratum,

    akkaActor,
    akkaContrib,
    akkaSlf4j,
    akkaHttp,
    akkaHttpSprayJson,
    akkaTestKit,
    akkaHttpTestKit,

    googleDataproc,
    googleRpc,
    googleOAuth2,
    googleSourceRepositories,

    scalaTest,
    mockito,

    slick,
    hikariCP,
    mysql,
    liquibase,

    workbenchUtil,
    workbenchModel,
    workbenchGoogle,
    workbenchGoogleTest,
    workbenchMetrics,
    sam,

    sealerate
  )

  val serviceTestV = "0.16-f0e5d47"
  val scalaV = "2.12"
  val excludeGuavaJdk5       = ExclusionRule(organization = "com.google.guava", name = "guava-jdk5")
  val excludeApacheHttpClient = ExclusionRule(organization = "org.apache.httpcomponents", name = "httpclient")
  val excludeGoogleJsr305    = ExclusionRule(organization = "com.google.code.findbugs", name = "jsr305")
  val excludeWorkbenchGoogle = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-google_" + scalaV)

  val workbenchServiceTest: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-service-test" % serviceTestV % "test" classifier "tests" excludeAll (
    excludeWorkbenchModel,
    excludeWorkbenchGoogle)

  val automationDependencies = List(
    // proactively pull in latest versions of Jackson libs, instead of relying on the versions
    // specified as transitive dependencies, due to OWASP DependencyCheck warnings for earlier versions.
    "com.fasterxml.jackson.core" % "jackson-annotations"  % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-databind"     % jacksonV excludeAll (excludeJacksonAnnotation),
    "com.fasterxml.jackson.core" % "jackson-core"         % jacksonV,
    "com.fasterxml.jackson.module" % ("jackson-module-scala_" + scalaV) % jacksonV,
    "ch.qos.logback"  % "logback-classic" % "1.2.3"  % "test",
    "com.google.apis" % "google-api-services-oauth2" % "v1-rev142-1.23.0" excludeAll (
      excludeGuavaJdk5,
      excludeApacheHttpClient,
      excludeGoogleJsr305,
      excludeJacksonCore),
    "com.google.api-client" % "google-api-client"   % "1.23.0" excludeAll (
      excludeGuavaJdk5,
      excludeApacheHttpClient,
      excludeGoogleJsr305,
      excludeJacksonCore),

    "com.typesafe.akka"   %%  "akka-http-core"      % akkaHttpV,
    "com.typesafe.akka"   %%  "akka-stream-testkit" % akkaV    % "test",
    "com.typesafe.akka"   %%  "akka-actor"          % akkaV,
    "com.typesafe.akka"   %%  "akka-http"           % akkaHttpV,
    "com.typesafe.akka"   %%  "akka-testkit"        % akkaV     % "test",
    "com.typesafe.akka"   %%  "akka-slf4j"          % akkaV,
    "org.scalatest"       %%  "scalatest"           % "3.0.5"   % "test",
    "org.seleniumhq.selenium" % "selenium-java"     % "3.14.0" % "test",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0",
    "org.apache.commons" % "commons-text"           % "1.2",
    "io.grpc" % "grpc-core" % "1.12.0" excludeAll(excludeGuava),

    workbenchUtil,
    workbenchModel,
    workbenchMetrics,
    workbenchGoogle,
    workbenchServiceTest,

    // required by workbenchGoogle
    "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.6" % "provided"
  )
}
