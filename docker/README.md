This directory contains build scripts and docker images that can be installed with Leonardo.

# Images

## Jupyter

Deprecated. Please see [terra-docker](https://github.com/DataBiosphere/terra-docker) for latest Leonardo images. 

Legacy image containing [Jupyter Notebooks](https://jupyter-notebook.readthedocs.io/en/stable/) and 
[JupyterLab](https://jupyterlab.readthedocs.io/en/latest/). Includes Python 2, Python 3, and R kernels. 
Also includes Hail 0.2. This image is not built anymore as of August 2019.

# Building

To build Leonardo itself, invoke `build.sh` from the leonardo root directory.
For example:
```
# Build Leonardo but don't push:
./docker/build.sh jar -d build
```
```
# Build Leonardo and push to a GCR repo:
./docker/build.sh jar -d push -gr us.gcr.io/broad-dsp-gcr-public -t my-awesome-tag
```
