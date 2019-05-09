#!/bin/sh

dockerd --experimental --rootless &

exec gosu $USER /usr/local/bin/jupyter notebook
