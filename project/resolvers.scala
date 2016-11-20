import sbt._

object Resolvers {
  val clojars= "clojars" at "http://clojars.org/repo/"
  val maven_local = Resolver.mavenLocal
  val snapshot_remote1 = "apache-snapshots" at "http://repository.apache.org/snapshots/"
}