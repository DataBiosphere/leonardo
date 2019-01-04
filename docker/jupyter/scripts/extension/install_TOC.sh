#!/usr/bin/env bash

set -e

pip3 install jupyter_contrib_nbextensions jupyter_nbextensions_configurator
sudo -E -u jupyter-user jupyter nbextensions_configurator enable --user
sudo -E -u jupyter-user jupyter contrib nbextension install --user
sudo -E -u jupyter-user jupyter nbextension enable toc2/main