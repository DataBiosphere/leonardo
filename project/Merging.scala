import sbtassembly.{MergeStrategy, PathList}

object Merging {
  def customMergeStrategy(oldStrategy: (String) => MergeStrategy):(String => MergeStrategy) = {
    case PathList("org", "joda", "time", "base", "BaseDateTime.class") => MergeStrategy.first
    case x => oldStrategy(x)
  }
}
