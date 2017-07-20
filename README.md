[![Build Status](https://travis-ci.org/broadinstitute/leonardo.svg?branch=develop)](https://travis-ci.org/broadinstitute/leonardo) [![Coverage Status](https://coveralls.io/repos/github/broadinstitute/leonardo/badge.svg?branch=develop)](https://coveralls.io/github/broadinstitute/leonardo?branch=develop)

# leonardo
Notebook service

## Building service

To build jar and docker image
```
./docker/build.sh jar -d build
```

To build jar and docker image, and push to `broadinstitute/leonardo` tagged with git hash
```
./docker/build.sh jar -d push
```
