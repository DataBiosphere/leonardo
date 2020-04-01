import sbtassembly.{MergeStrategy, PathList}

object Merging {
  def customMergeStrategy(oldStrategy: (String) => MergeStrategy):(String => MergeStrategy) = {
    case PathList("org", "joda", "time", "base", "BaseDateTime.class") => MergeStrategy.first
    case "module-info.class" => MergeStrategy.discard  // JDK 8 does not use the file module-info.class so it is safe to discard the file.
    case "reference.conf" => MergeStrategy.concat
    case x => oldStrategy(x)
  }
}
