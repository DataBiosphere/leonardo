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
c.NotebookApp.disable_check_xsrf = True #see https://github.com/nteract/hydrogen/issues/922
c.NotebookApp.allow_origin = '*'

c.NotebookApp.terminado_settings={'shell_command': ['bash']}

fragment = os.environ['GOOGLE_PROJECT'] + '/' + os.environ['CLUSTER_NAME']
c.NotebookApp.base_url = '/notebooks/' + fragment + '/'

# This is also specified in run-jupyter.sh
c.NotebookApp.nbserver_extensions = {
    'jupyter_localize_extension': True
}
c.NotebookApp.contents_manager_class = 'jupyter_delocalize.DelocalizingContentsManager'

# Unset Content-Security-Policy so Jupyter can be rendered in an iframe
# See https://jupyter-notebook.readthedocs.io/en/latest/public_server.html?highlight=server#embedding-the-notebook-in-another-website
c.NotebookApp.tornado_settings = {
    'static_url_prefix':'/notebooks/' + fragment + '/static/',
    'headers': {
        'Content-Security-Policy': "frame-ancestors 'self' http://localhost:3000 https://bvdp-saturn-prod.appspot.com https://bvdp-saturn-dev.appspot.com; default-src 'self' ; script-src https://*.broadinstitute.org "
    }
}
