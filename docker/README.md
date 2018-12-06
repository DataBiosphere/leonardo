This directory contains build scripts and docker images that can be installed with Leonardo.

# Images

## Jupyter

Image containing [Jupyter Notebooks](https://jupyter-notebook.readthedocs.io/en/stable/) and 
[JupyterLab](https://jupyterlab.readthedocs.io/en/latest/). Includes Python 2, Python 3, and R kernels. 
Also includes Hail 0.2. For more information, see here.

## RStudio

Image containing RStudio Server. Extends [rocker/tidyverse](https://hub.docker.com/r/rocker/tidyverse/)
to bring in tidyverse and other R packages. Also includes google-cloud-sdk and google-cloud libraries for R.

# Building

To build a specific image, invoke `build-tool.sh` from the leonardo root directory. For example:
```
# build & tag Jupyter image and push to GCR repo
./docker/build-tool.sh --push -i jupyter -r us.gcr.io/broad-dsp-gcr-public -t my-awesome-tag
```
```
# build an RStudio image but don't push
./docker/build-tool.sh -i rstudio -r us.gcr.io/broad-dsp-gcr-public -t my-tag
```

To build Leonardo itself plus all supported images, invoke `build.sh` from the leonardo root directory.
For example:
```
# Build Leonardo, Jupyter, and RStudio images but don't push:
./docker/build.sh jar -d build
```
```
# Build Leonardo, Jupyter, and RStudio images and push to a GCR repo:
./docker/build.sh jar -d push -gr us.gcr.io/broad-dsp-gcr-public -t my-awesome-tag
```