# pulled from https://hub.docker.com/r/ansingh7115/leonardo-notebooks/
# and added base_url configs for local proxy testing

from jupyter_core.paths import jupyter_data_dir
import subprocess
import os
import errno
import stat

c = get_config()
c.NotebookApp.ip = '*'
c.NotebookApp.port = 8000
c.NotebookApp.open_browser = False
c.NotebookApp.base_url = '/notebooks/test/'
c.NotebookApp.webapp_settings = {'static_url_prefix':'/notebooks/test/static/'}

# set password to 'password' TODO security lol
