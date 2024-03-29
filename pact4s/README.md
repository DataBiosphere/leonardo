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

If you are already using OpenJDK 17, run the following command. 
```
$ sbt "project pact4s" clean test  
```

Otherwise, you can run the command inside a docker container with OpenJDK 17 installed. 
This is especially useful when automating contract tests in a GitHub Action runner which does not guarantee the correct OpenJDK version.
```
docker run --rm -v $PWD:/working \
                -v jar-cache:/root/.ivy \
                -v jar-cache:/root/.ivy2 \
                -w /working \
                sbtscala/scala-sbt:openjdk-17.0.2_1.7.2_2.13.10 \
                sbt "project pact4s" clean test
```

The generated contracts can be found in the `./target/pacts` folder
- `leo-consumer-sam-provider.json`
- `leo-consumer-fake-provider.json`

