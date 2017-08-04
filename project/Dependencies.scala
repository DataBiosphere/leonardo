import sbt._

object Dependencies {
  val akkaV         = "2.5.3"
  val akkaHttpV     = "10.0.9"
  val jacksonV      = "2.8.7"
  val googleV       = "1.22.0"
  val scalaLoggingV = "3.7.2"
  val scalaTestV    = "3.0.1"

  val workbenchUtilV   = "0.2-f87e766"
  val workbenchModelV  = "0.1-6924e2f"
  val workbenchGoogleV = "0.1-6924e2f"

  val excludeAllAkka =          ExclusionRule(organization = "com.typesafe.akka")
  val excludeAkkaActor =        ExclusionRule(organization = "com.typesafe.akka", name = "akka-actor_2.12")
  val excludeGuavaJDK5 =        ExclusionRule(organization = "com.google.guava", name = "guava-jdk5")
  val excludeWorkbenchUtil =    ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-util_2.12")
  val excludeWorkbenchModel =   ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-model_2.12")
  val excludeWorkbenchMetrics = ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-metrics_2.12")

  val jacksonAnnotations: ModuleID = "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonV
  val jacksonDatabind: ModuleID =    "com.fasterxml.jackson.core" % "jackson-databind"    % jacksonV
  val jacksonCore: ModuleID =        "com.fasterxml.jackson.core" % "jackson-core"        % jacksonV

  val logbackClassic: ModuleID = "ch.qos.logback"             %  "logback-classic" % "1.2.1"
  val ravenLogback: ModuleID =   "com.getsentry.raven"        %  "raven-logback"   % "7.8.6"
  val scalaLogging: ModuleID =   "com.typesafe.scala-logging" %% "scala-logging"   % scalaLoggingV
  val swaggerUi: ModuleID =      "org.webjars"                %  "swagger-ui"      % "2.2.5"
  val ficus: ModuleID =          "com.iheart"                 %% "ficus"           % "1.4.0"

  val akkaActor: ModuleID =         "com.typesafe.akka"   %%  "akka-actor"           % akkaV
  val akkaContrib: ModuleID =       "com.typesafe.akka"   %%  "akka-contrib"         % akkaV
  val akkaSlf4j: ModuleID =         "com.typesafe.akka"   %%  "akka-slf4j"           % akkaV
  val akkaHttp: ModuleID =          "com.typesafe.akka"   %%  "akka-http"            % akkaHttpV           excludeAll(excludeAkkaActor)
  val akkaHttpSprayJson: ModuleID = "com.typesafe.akka"   %%  "akka-http-spray-json" % akkaHttpV
  val akkaTestKit: ModuleID =       "com.typesafe.akka"   %%  "akka-testkit"         % akkaV     % "test"
  val akkaHttpTestKit: ModuleID =   "com.typesafe.akka"   %%  "akka-http-testkit"    % akkaHttpV % "test"

  val googleDataproc: ModuleID =    "com.google.apis"     % "google-api-services-dataproc" % s"v1-rev53-$googleV" excludeAll(excludeGuavaJDK5)
  
  val scalaTest: ModuleID = "org.scalatest" %% "scalatest" % scalaTestV % "test"

  // All of workbench-libs pull in Akka; exclude it since we provide our own Akka dependency.
  // workbench-google pulls in workbench-{util, model, metrics}; exclude them so we can control the library versions individually.
  val workbenchUtil: ModuleID =      "org.broadinstitute.dsde.workbench" %% "workbench-util"   % workbenchUtilV   excludeAll(excludeAllAkka)
  val workbenchModel: ModuleID =     "org.broadinstitute.dsde.workbench" %% "workbench-model"  % workbenchModelV  excludeAll(excludeAllAkka)
  val workbenchGoogle: ModuleID =    "org.broadinstitute.dsde.workbench" %% "workbench-google" % workbenchGoogleV excludeAll(excludeAllAkka, excludeWorkbenchUtil, excludeWorkbenchModel, excludeWorkbenchMetrics)

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

    akkaActor,
    akkaContrib,
    akkaSlf4j,
    akkaHttp,
    akkaHttpSprayJson,
    akkaTestKit,
    akkaHttpTestKit,

    googleDataproc,

    scalaTest,

    workbenchUtil,
    workbenchModel,
    workbenchGoogle
  )
}
