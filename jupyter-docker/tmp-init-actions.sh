#!/usr/bin/env bash

set -e -x


apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev

export PYENV_ROOT="/root/.pyenv"
export PATH="/root/.pyenv/bin:$PATH"

curl -L https://raw.githubusercontent.com/yyuu/pyenv-installer/master/bin/pyenv-installer | bash

eval "$(/root/.pyenv/bin/pyenv init -)"

/root/.pyenv/bin/pyenv install 2.7.13
/root/.pyenv/bin/pyenv install 3.5.3
/root/.pyenv/bin/pyenv global 2.7.13 3.5.3