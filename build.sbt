name := "citation-search"

organization := "net.tqft"

version := "0.1"

scalaVersion := "2.10.3"

scalacOptions += "-target:jvm-1.7"

resolvers ++= Seq(
	"Java.net Maven2 Repository" at "http://download.java.net/maven/2/",
	"Sonatype Nexus Releases" at "https://oss.sonatype.org/content/repositories/releases",
	"Scala Snapshots" at "http://scala-tools.org/repo-snapshots/",
        "twitter-repo" at "http://maven.twttr.com"
)

// Project dependencies
libraryDependencies ++= Seq(
        "com.twitter" %% "finagle-core" % "6.1.0",
        "com.twitter" %% "finagle-http" % "6.1.0",
        "com.github.theon" %% "scala-uri" % "0.3.4"
)

// Test dependencies
libraryDependencies ++= Seq(
	"junit" % "junit" % "4.8" % "test",
	"org.scalatest" % "scalatest_2.10" % "2.0" % "test"
)

publishTo := Some(Resolver.sftp("tqft.net", "tqft.net", "tqft.net/releases") as ("scottmorrison", new java.io.File("/Users/scott/.ssh/id_rsa")))

