# Example notebooks

This folder contains some example notebooks that can be used in Leonardo. These notebooks are purely for demo purposes; they do not contain any sensitive data.

## Running

To try out an example notebook, download it to your machine, open Jupyter, and select Upload.

![Upload a notebook](upload.png) 

## List

Here is a current listing of example notebooks. Please keep this up to date if you add more.

1. [Hail_Tutorial.ipynb](https://github.com/DataBiosphere/leonardo/tree/develop/example-notebooks/Hail_Tutorial.ipynb)
   - Contains the Hail 0.1 (stable) tutorial, adapted from here: https://hail.is/docs/stable/tutorials/hail-overview.html
2. [LeonarDemo.ipynb](https://github.com/DataBiosphere/leonardo/tree/develop/example-notebooks/LeonarDemo.ipynb)
   - Python 2 sample notebook which loads the FireCloud data model into pandas objects, interacts with Google Cloud Storage APIs, and runs a basic Hail job.
      - NOTE: references a demo workspace which no longer exists, so won't be runnable as-is.
3. [Notebooks_Demo_Manning_Lab.ipynb](https://github.com/DataBiosphere/leonardo/tree/develop/example-notebooks/Notebooks_Demo_Manning_Lab.ipynb)
   - Python 2 notebook which runs an analysis of publicly available 1000 genomes data. Uses FISS for interacting with a FireCloud workspace and pandas and numpy for manipulating data. Runs Hail 0.1 to do some analysis.
   - Developed by Alisa Manning's team for demonstration purposes.
4. [Safari_Online_Jupyter_Training.ipynb](https://github.com/DataBiosphere/leonardo/tree/develop/example-notebooks/Safari_Online_Jupyter_Training.ipynb)
   - Adaptation of [Safari online Jupyter training](https://www.safaribooksonline.com/public/online-training-jupyter/) which exercises some basic tools like pandas, numpy, and matplotlib in Python 3. 

NEEDED: example notebook using R and bigquery.
