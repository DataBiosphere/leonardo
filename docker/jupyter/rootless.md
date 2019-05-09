## Docker in Rootless Notebooks

Jupyter Notebooks can optionally run in a [rootless Docker container](https://engineering.docker.com/2019/02/experimenting-with-rootless-docker/), to provide additional protection for the host.

However, we also use *rootless mode* to provide a safer way for users
to build/run their own Docker containers *inside* notebooks.

This mode works by providing a custom binary for the Docker daemon and associated executables,
and should be considered an experimental feature.

To set it up, first build and push a custom Docker image,
```
docker build -t us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter:rootless -f Dockerfile.rootless .
docker push us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter:rootless
```

Next, use an **Ubuntu 18.04+** VM for the master node
(other distributions place severe limitations on filesystem performance).
Run the following as a *non-root* user on the VM:
```
DOCKER_IMG=us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter:rootless \
  ROOTLESS=1 ./docker/jupyter/run-jupyter-local.sh start
```

This command downloads and starts experimental Docker daemon, as the user who calls it.
Then, the Docker executables are mounted into the notebook ("host") container to provide a way
for Jupyter users to run Docker commands from *inside* the host container.

Note that the host container starts a *separate* "rootful" Docker daemon *inside* of itself,
so that any "child" containers started by the user don't affect the host.

In summary, the flow of control is as follows:

Rootless Docker daemon → Notebook container → Rootful Docker daemon → Child/User containers

As a result, notebook users can just run *regular* docker commands, such as
`docker build` and `docker run`, with all of the usual semantics involved.
