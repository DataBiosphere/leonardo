# adapted from https://github.com/jupyter/docker-stacks/blob/master/base-notebook/jupyter_notebook_config.py

from jupyter_core.paths import jupyter_data_dir
import subprocess
import os
import errno
import stat

c = get_config()
c.NotebookApp.ip = 'localhost'
c.NotebookApp.port = 8000
c.NotebookApp.open_browser = False

c.NotebookApp.token = ''
c.NotebookApp.disable_check_xsrf = True #see https://github.com/nteract/hydrogen/issues/922
c.NotebookApp.allow_origin = '*'

c.NotebookApp.terminado_settings={'shell_command': ['bash']}

fragment = os.environ['GOOGLE_PROJECT'] + '/' + os.environ['CLUSTER_NAME']
c.NotebookApp.base_url = '/notebooks/' + fragment + '/'

# Using an alternate notebooks_dir allows for mounting of a shared volume here.
# The default notebook root dir is /home/jupyter-user, which is also the home
# directory (which contains several other files on the image). We don't want
# to mount there as it effectively deletes existing files on the image.
# See https://docs.google.com/document/d/1b8wIydzC4D7Sbb6h2zWe-jCvoNq-bbD02CT1cnKLbGk
if os.environ['WELDER_ENABLE'] == 'true':
  # Only enable this along with Welder, as this change is backwards incompatible
  # for older localization users.
  c.NotebookApp.notebook_dir = '/home/jupyter-user/notebooks'

# This is also specified in run-jupyter.sh
c.NotebookApp.nbserver_extensions = {
    'jupyter_localize_extension': True
}
mgr_class = 'DelocalizingContentsManager'
if os.environ['WELDER_ENABLE'] == 'true':
  mgr_class = 'WelderContentsManager'
c.NotebookApp.contents_manager_class = 'jupyter_delocalize.' + mg_class

# Unset Content-Security-Policy so Jupyter can be rendered in an iframe
# See https://jupyter-notebook.readthedocs.io/en/latest/public_server.html?highlight=server#embedding-the-notebook-in-another-website
c.NotebookApp.tornado_settings = {
    'static_url_prefix':'/notebooks/' + fragment + '/static/',
    'headers': {
        'Content-Security-Policy': $(contentSecurityPolicy)
    }
}
