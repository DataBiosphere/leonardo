import sbtassembly.{MergeStrategy, PathList}

object Merging {
  def customMergeStrategy(oldStrategy: (String) => MergeStrategy): (String => MergeStrategy) = {
    // For the following error:
    // [error] Deduplicate found different file contents in the following:
    // [error]   Jar name = auto-value-1.10.1.jar, jar org = com.google.auto.value, entry target = META-INF/kotlin-stdlib.kotlin_module
    // [error]   Jar name = kotlin-stdlib-1.6.20.jar, jar org = org.jetbrains.kotlin, entry target = META-INF/kotlin-stdlib.kotlin_module
    case PathList("META-INF", "kotlin-stdlib.kotlin_module") => MergeStrategy.preferProject
    case PathList("META-INF", "okio.kotlin_module")          => MergeStrategy.first
    // For the following error:
    // [error] Deduplicate found different file contents in the following:
    // [error]   Jar name = auto-value-1.10.1.jar, jar org = com.google.auto.value, entry target = META-INF/kotlin-stdlib-common.kotlin_module
    // [error]   Jar name = kotlin-stdlib-1.6.20.jar, jar org = org.jetbrains.kotlin, entry target = META-INF/kotlin-stdlib-common.kotlin_module
    case PathList("META-INF", "kotlin-stdlib-common.kotlin_module")    => MergeStrategy.preferProject
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
    // For the following error:
    // Error:  Deduplicate found different file contents in the following:
    // Error:    Jar name = bcpkix-jdk18on-1.78.jar, jar org = org.bouncycastle, entry target = META-INF/versions/9/OSGI-INF/MANIFEST.MF
    // Error:    Jar name = bcprov-jdk18on-1.78.jar, jar org = org.bouncycastle, entry target = META-INF/versions/9/OSGI-INF/MANIFEST.MF
    // Error:    Jar name = bcutil-jdk18on-1.78.jar, jar org = org.bouncycastle, entry target = META-INF/versions/9/OSGI-INF/MANIFEST.MF
    case x if x.endsWith("/OSGI-INF/MANIFEST.MF") =>
      MergeStrategy.first
    case x if x.contains("bouncycastle") =>
      MergeStrategy.first
    case "module-info.class" =>
      MergeStrategy.discard // JDK 8 does not use the file module-info.class so it is safe to discard the file.
    case "reference.conf" => MergeStrategy.concat
    // For the following error:
    // [error] Deduplicate found different file contents in the following:
    // [error]   Jar name = jakarta.activation-1.2.2.jar, jar org = com.sun.activation, entry target = javax/activation/SecuritySupport$5.class
    // [error]   Jar name = javax.activation-api-1.2.0.jar, jar org = javax.activation, entry target = javax/activation/SecuritySupport$5.class
    // case PathList("META-INF", "kotlin-stdlib-common.kotlin_module")    => MergeStrategy.preferProject
    case x if x.contains("activation") => MergeStrategy.first
    case x if x.contains("annotation") => MergeStrategy.first
    case "logback.xml"                 => MergeStrategy.first
    case x                             => oldStrategy(x)
  }
}
