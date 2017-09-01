# adapted from https://github.com/jupyter/docker-stacks/blob/master/base-notebook/jupyter_notebook_config.py

from jupyter_core.paths import jupyter_data_dir
import subprocess
import os
import errno
import stat

c = get_config()
c.NotebookApp.ip = '*'
c.NotebookApp.port = 8000
c.NotebookApp.open_browser = False
c.NotebookApp.token = ''
c.NotebookApp.allow_origin = '*'

fragment = os.environ['GOOGLE_PROJECT'] + '/' + os.environ['CLUSTER_NAME']
c.NotebookApp.base_url = '/api/notebooks/' + fragment + '/'
c.NotebookApp.webapp_settings = {'static_url_prefix':'/api/notebooks/' + fragment + '/static/'}