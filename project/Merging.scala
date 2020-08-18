import sbtassembly.{MergeStrategy, PathList}

object Merging {
  def customMergeStrategy(oldStrategy: (String) => MergeStrategy): (String => MergeStrategy) = {
    case PathList("org", "joda", "time", "base", "BaseDateTime.class") => MergeStrategy.first
    // For the following error:
    //[error] java.lang.RuntimeException: deduplicate: different file contents found in the following:
    //[error] /root/.cache/coursier/v1/https/repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.11.4/protobuf-java-3.11.4.jar:google/protobuf/field_mask.proto
    //[error] /root/.cache/coursier/v1/https/repo1.maven.org/maven2/com/typesafe/akka/akka-protobuf-v3_2.12/2.6.5/akka-protobuf-v3_2.12-2.6.5.jar:google/protobuf/field_mask.proto
    case PathList("google", "protobuf", "field_mask.proto")         => MergeStrategy.first
    case PathList("google", "protobuf", "descriptor.proto")         => MergeStrategy.first
    case PathList("google", "protobuf", "compiler", "plugin.proto") => MergeStrategy.first
    case "module-info.class" =>
      MergeStrategy.discard // JDK 8 does not use the file module-info.class so it is safe to discard the file.
    case "reference.conf" => MergeStrategy.concat
    // For the following error:
    //[error] (assembly) deduplicate: different file contents found in the following:
    //[error] logback.xml
    //[error] /Users/rtitle/Library/Caches/Coursier/v1/https/broadinstitute.jfrog.io/broadinstitute/libs-release/org/broadinstitute/dsp/helm-scala-sdk_2.12/0.0.2-SNAPSHOT/helm-scala-sdk_2.12-0.0.2-SNAPSHOT.jar:logback.xml
    // The Leo logback.xml should come first in the classpath, so okay to exclude the helm-scala-sdk one.
    case "logback.xml" => MergeStrategy.first
    case x             => oldStrategy(x)
  }
}
