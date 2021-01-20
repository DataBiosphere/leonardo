# adapted from https://github.com/jupyter/docker-stacks/blob/master/base-notebook/jupyter_notebook_config.py
# Note: this file also lives in the terra-jupyter-base image in the terra-docker repo.
# If you change this please keep the other version consistent as well.

import os

c = get_config()
c.NotebookApp.ip = '0.0.0.0'
c.NotebookApp.port = 8000
c.NotebookApp.open_browser = False

c.NotebookApp.token = ''
c.NotebookApp.disable_check_xsrf = True #see https://github.com/nteract/hydrogen/issues/922
c.NotebookApp.allow_origin = '*'

c.NotebookApp.terminado_settings={'shell_command': ['bash']}

if 'GOOGLE_PROJECT' in os.environ and 'CLUSTER_NAME' in os.environ:
  fragment = '/' + os.environ['GOOGLE_PROJECT'] + '/' + os.environ['CLUSTER_NAME'] + '/'
else:
  fragment = '/'

c.NotebookApp.base_url = '/notebooks' + fragment

# Using an alternate notebooks_dir allows for mounting of a shared volume here.
# The default notebook root dir is /home/jupyter-user, which is also the home
# directory (which contains several other files on the image). We don't want
# to mount there as it effectively deletes existing files on the image.
# See https://docs.google.com/document/d/1b8wIydzC4D7Sbb6h2zWe-jCvoNq-bbD02CT1cnKLbGk
if os.environ.get('WELDER_ENABLED') == 'true':
  # Only enable this along with Welder, as this change is backwards incompatible
  # for older localization users.
  c.NotebookApp.notebook_dir = os.environ.get('NOTEBOOKS_DIR', '/home/jupyter-user/notebooks')

# This is also specified in run-jupyter.sh
c.NotebookApp.nbserver_extensions = {
    'jupyter_localize_extension': True
}

mgr_class = 'DelocalizingContentsManager'
if os.environ.get('WELDER_ENABLED') == 'true':
  mgr_class = 'WelderContentsManager'
c.NotebookApp.contents_manager_class = 'jupyter_delocalize.' + mgr_class

c.NotebookApp.tornado_settings = {
    'static_url_prefix':'/notebooks' + fragment + 'static/'
}
