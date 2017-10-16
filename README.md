[![Build Status](https://travis-ci.org/broadinstitute/leonardo.svg?branch=develop)](https://travis-ci.org/broadinstitute/leonardo) [![Coverage Status](https://coveralls.io/repos/github/broadinstitute/leonardo/badge.svg?branch=develop)](https://coveralls.io/github/broadinstitute/leonardo?branch=develop)

# leonardo
Notebook service for Workbench

## Getting started
Clone the repo.
```
$ git clone https://github.com/broadinstitute/leonardo.git
$ cd leonardo
```
Ensure docker is running. Spin up MySQL locally:
```
$ ./docker/run-mysql.sh start leonardo
```
Build Leonardo and run tests.
```
export SBT_OPTS="-Xmx2G -Xms1G -Dmysql.host=localhost -Dmysql.port=3311"
sbt clean compile test
```
Once you're done, tear down MySQL.
```
./docker/run-mysql.sh stop leonardo
```

## Building Leonardo docker image

To install git-secrets
```$xslt
brew install git-secrets
```
To ensure git hooks are run
```$xslt
cp -r hooks/ .git/hooks/
chmod 755 .git/hooks/apply-git-secrets.sh
```

To build jar, leonardo docker image, and leonardo-notebooks docker image
```
./docker/build.sh jar -d build
```

To build jar, leonardo docker image, and leonardo-notebooks docker image 
and push to repos `broadinstitute/leonardo` and `broadinstitute/leonardo-notebooks` 
tagged with git hash
```
./docker/build.sh jar -d push
```

To build the leonardo-notebooks docker image with a given tag
````
bash ./jupyter-docker/build.sh build <TAG NAME>
````

To push the leonardo-notebooks docker image you built
to repo `broadinstitute/leonardo-notebooks`

````
bash ./jupyter-docker/build.sh push <TAG NAME>
````
