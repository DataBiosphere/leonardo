import sbtassembly.{MergeStrategy, PathList}

object Merging {
  def customMergeStrategy(oldStrategy: (String) => MergeStrategy): (String => MergeStrategy) = {
    case PathList("META-INF", "okio.kotlin_module") => MergeStrategy.first
    case PathList("META-INF", "versions", _ @_*)   => MergeStrategy.first // Multi-Release JAR paths (e.g. BouncyCastle OSGi manifests)
    case PathList("google", "protobuf", _ @_*)      => MergeStrategy.first
    case x if x.endsWith("/module-info.class")      => MergeStrategy.discard
    case "module-info.class" =>
      MergeStrategy.discard // JDK 8 does not use the file module-info.class so it is safe to discard the file.
    case "reference.conf" => MergeStrategy.concat
    case x                => oldStrategy(x)
  }
}
