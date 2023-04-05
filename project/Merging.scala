import sbtassembly.{MergeStrategy, PathList}

object Merging {
  def customMergeStrategy(oldStrategy: (String) => MergeStrategy): (String => MergeStrategy) = {
    // For the following error:
    // [error] Deduplicate found different file contents in the following:
    // [error]   Jar name = auto-value-1.10.1.jar, jar org = com.google.auto.value, entry target = META-INF/kotlin-stdlib.kotlin_module
    // [error]   Jar name = kotlin-stdlib-1.6.20.jar, jar org = org.jetbrains.kotlin, entry target = META-INF/kotlin-stdlib.kotlin_module
    case PathList("META-INF", "kotlin-stdlib", "kotlin_module", _ @ _*) => MergeStrategy.preferProject
    // For the following error:
    // [error] Deduplicate found different file contents in the following:
    // [error]   Jar name = auto-value-1.10.1.jar, jar org = com.google.auto.value, entry target = META-INF/kotlin-stdlib-common.kotlin_module
    // [error]   Jar name = kotlin-stdlib-1.6.20.jar, jar org = org.jetbrains.kotlin, entry target = META-INF/kotlin-stdlib-common.kotlin_module
    case PathList("META-INF", "kotlin-stdlib-common", "kotlin_module", _ @ _*) => MergeStrategy.preferProject
    case PathList("org", "joda", "time", "base", "BaseDateTime.class") => MergeStrategy.first
    // For the following error:
    // [error] java.lang.RuntimeException: deduplicate: different file contents found in the following:
    // [error] /root/.cache/coursier/v1/https/repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.11.4/protobuf-java-3.11.4.jar:google/protobuf/field_mask.proto
    // [error] /root/.cache/coursier/v1/https/repo1.maven.org/maven2/com/typesafe/akka/akka-protobuf-v3_2.12/2.6.5/akka-protobuf-v3_2.12-2.6.5.jar:google/protobuf/field_mask.proto
    case PathList("google", "protobuf", _ @_*)           => MergeStrategy.first
    case PathList("javax", "xml", _ @_*)                 => MergeStrategy.first
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
    case x if x.endsWith("/ModuleUtil.class")            => MergeStrategy.first
    case x if x.endsWith("/module-info.class")           => MergeStrategy.discard
    case x if x.contains("bouncycastle") =>
      MergeStrategy.first
    case "module-info.class" =>
      MergeStrategy.discard // JDK 8 does not use the file module-info.class so it is safe to discard the file.
    case "reference.conf" => MergeStrategy.concat
    case x                => oldStrategy(x)
  }
}
