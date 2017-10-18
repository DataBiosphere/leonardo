import sbt._

object Dependencies {
  val akkaV         = "2.5.3"
  val akkaHttpV     = "10.0.9"
  val jacksonV      = "2.9.0"
  val googleV       = "1.22.0"
  val scalaLoggingV = "3.7.2"
  val scalaTestV    = "3.0.1"
  val slickV        = "3.2.1"

  val workbenchUtilV    = "0.2-1b977d7"
  val workbenchModelV   = "0.3-b23a91c"
  val workbenchGoogleV  = "0.4-c91bfa1"
  val workbenchMetricsV = "0.3-1b977d7"

  val excludeAkkaActor =        ExclusionRule(organization = "com.typesafe.akka", name = "akka-actor_2.12")
  val excludeGuavaJDK5 =        ExclusionRule(organization = "com.google.guava", name = "guava-jdk5")
  val excludeWorkbenchUtil =    ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-util_2.12")
  val excludeWorkbenchModel =   ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-model_2.12")
  val excludeWorkbenchMetrics = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-metrics_2.12")

  val jacksonAnnotations: ModuleID = "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonV
  val jacksonDatabind: ModuleID =    "com.fasterxml.jackson.core" % "jackson-databind"    % jacksonV
  val jacksonCore: ModuleID =        "com.fasterxml.jackson.core" % "jackson-core"        % jacksonV

  val logbackClassic: ModuleID = "ch.qos.logback"             %  "logback-classic" % "1.2.2"
  val ravenLogback: ModuleID =   "com.getsentry.raven"        %  "raven-logback"   % "7.8.6"
  val scalaLogging: ModuleID =   "com.typesafe.scala-logging" %% "scala-logging"   % scalaLoggingV
  val swaggerUi: ModuleID =      "org.webjars"                %  "swagger-ui"      % "2.2.5"
  val ficus: ModuleID =          "com.iheart"                 %% "ficus"           % "1.4.0"
  val cats: ModuleID =           "org.typelevel"              %% "cats"            % "0.9.0"
  val httpClient: ModuleID =     "org.apache.httpcomponents"  % "httpclient"       % "4.5.3"  // upgrading a transitive dependency to avoid security warnings

  val akkaActor: ModuleID =         "com.typesafe.akka"   %%  "akka-actor"           % akkaV
  val akkaContrib: ModuleID =       "com.typesafe.akka"   %%  "akka-contrib"         % akkaV
  val akkaSlf4j: ModuleID =         "com.typesafe.akka"   %%  "akka-slf4j"           % akkaV
  val akkaHttp: ModuleID =          "com.typesafe.akka"   %%  "akka-http"            % akkaHttpV           excludeAll(excludeAkkaActor)
  val akkaHttpSprayJson: ModuleID = "com.typesafe.akka"   %%  "akka-http-spray-json" % akkaHttpV
  val akkaTestKit: ModuleID =       "com.typesafe.akka"   %%  "akka-testkit"         % akkaV     % "test"
  val akkaHttpTestKit: ModuleID =   "com.typesafe.akka"   %%  "akka-http-testkit"    % akkaHttpV % "test"

  val googleDataproc: ModuleID =    "com.google.apis"     % "google-api-services-dataproc" % s"v1-rev53-$googleV" excludeAll(excludeGuavaJDK5)
  val googleRpc: ModuleID = "io.grpc" % "grpc-core" % "1.5.0"

  val scalaTest: ModuleID = "org.scalatest" %% "scalatest"    % scalaTestV % "test"
  val mockito: ModuleID =   "org.mockito"    % "mockito-core" % "2.7.22"   % "test"

  // Exclude workbench-libs transitive dependencies so we can control the library versions individually.
  // workbench-google pulls in workbench-{util, model, metrics} and workbench-metrics pulls in workbench-util.
  val workbenchUtil: ModuleID =      "org.broadinstitute.dsde.workbench" %% "workbench-util"    % workbenchUtilV
  val workbenchModel: ModuleID =     "org.broadinstitute.dsde.workbench" %% "workbench-model"   % workbenchModelV
  val workbenchGoogle: ModuleID =    "org.broadinstitute.dsde.workbench" %% "workbench-google"  % workbenchGoogleV excludeAll(excludeWorkbenchUtil, excludeWorkbenchModel, excludeWorkbenchMetrics)
  val workbenchMetrics: ModuleID =   "org.broadinstitute.dsde.workbench" %% "workbench-metrics" % workbenchMetricsV excludeAll(excludeWorkbenchUtil)

  val slick: ModuleID =     "com.typesafe.slick" %% "slick"                 % slickV
  val hikariCP: ModuleID =  "com.typesafe.slick" %% "slick-hikaricp"        % slickV
  val mysql: ModuleID =     "mysql"               % "mysql-connector-java"  % "6.0.6"
  val liquibase: ModuleID = "org.liquibase"       % "liquibase-core"        % "3.5.3"

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
    cats,
    httpClient,

    akkaActor,
    akkaContrib,
    akkaSlf4j,
    akkaHttp,
    akkaHttpSprayJson,
    akkaTestKit,
    akkaHttpTestKit,

    googleDataproc,
    googleRpc,

    scalaTest,
    mockito,

    slick,
    hikariCP,
    mysql,
    liquibase,

    workbenchUtil,
    workbenchModel,
    workbenchGoogle,
    workbenchMetrics
  )
}
