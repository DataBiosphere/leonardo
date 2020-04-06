import sbtassembly.{MergeStrategy, PathList}

object Merging {
  def customMergeStrategy(oldStrategy: (String) => MergeStrategy):(String => MergeStrategy) = {
    case PathList("org", "joda", "time", "base", "BaseDateTime.class") => MergeStrategy.first
    case "module-info.class" => MergeStrategy.discard  // JDK 8 does not use the file module-info.class so it is safe to discard the file.
    case "reference.conf" => MergeStrategy.concat
//  [error] /Users/qi/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.11.4/protobuf-java-3.11.4.jar:google/protobuf/field_mask.proto
//  [error] /Users/qi/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/typesafe/akka/akka-protobuf-v3_2.12/2.6.1/akka-protobuf-v3_2.12-2.6.1.jar:google/protobuf/field_mask.proto
    case "field_mask.proto" => MergeStrategy.first //Fix the error above
    case x => oldStrategy(x)
  }
}
