name := "LiveLib personal books warehouse"

version := "1.0"

scalaVersion := "2.12.17"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  "spark_transformations." + artifact.extension
}

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"
libraryDependencies += "org.postgresql" % "postgresql" % "42.6.0"