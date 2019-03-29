# This script runs at kernel startup time and sets environment variables for the
# workspace name and workspace bucket.
#
# Note: this script is highly dependent on a convention used by Terra and AllOfUs
# applications to place notebooks in the following directory structure:
#
#   ~jupyter-user/<workspace-name>/notebook.ipynb
#
# It exploits the fact that the CWD of a launching notebook is named after the workspace.
# If notebooks are ever launched from other directories, this script will break.

import fapi
import os

# The workspace name is simply the CWD of the running notebook.
cwd = os.getcwd()
workspace = os.path.basename(cwd)
os.environ['WORKSPACE_NAME'] = workspace

# Invoke FISS to retrieve the workspace bucket based on the workspace name
ws = fapi.get_workspace(os.environ['WORKSPACE_NAMESPACE'], os.environ['WORKSPACE_NAME']).json().get('workspace',{})
if 'bucketName' in ws:
    os.environ['WORKSPACE_BUCKET'] = ws['bucketName']