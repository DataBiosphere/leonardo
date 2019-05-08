#!/bin/bash

pip3 install igv-jupyter

jupyter serverextension enable --py igv --sys-prefix
jupyter nbextension install --py igv --sys-prefix
jupyter nbextension enable --py igv --sys-prefix


