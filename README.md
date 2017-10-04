[![Build Status](https://travis-ci.org/broadinstitute/leonardo.svg?branch=develop)](https://travis-ci.org/broadinstitute/leonardo) [![Coverage Status](https://coveralls.io/repos/github/broadinstitute/leonardo/badge.svg?branch=develop)](https://coveralls.io/github/broadinstitute/leonardo?branch=develop)

# leonardo
Notebook service

## Building service

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

To build the leonardo-notebooks docker image 
````
./jupyter-docker/build.sh build <TAG NAME>
````

To push the leonardo-notebooks docker image to repo `broadinstitute/leonardo-notebooks`
tagged with git hash
````
./jupyter-docker/build.sh push <TAG NAME>
````