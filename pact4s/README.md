# pact4s [Under construction]

pact4s is used for contract testing.

# Dependencies

```scala
  val pact4sDependencies = Seq(
    pact4sScalaTest,
    pact4sCirce,
    http4sEmberClient,
    http4sDsl,
    http4sEmberServer,
    http4sCirce,
    circeCore,
    typelevelCat,
    scalaTest
  )

lazy val pact4s = project.in(file("pact4s"))
  .settings(pact4sSettings)
  .dependsOn(http % "test->test;compile->compile")
```

## Building and running contract tests
Clone the repo.
```
$ git clone https://github.com/DataBiosphere/leonardo.git 
$ cd leonardo
```

```
$ sbt "project pact4s" test  
```

Generated contract can be found in the ./target/pacts folder: `sam-consumer-sam-provider.json`.

