# Jupyter server config file for Azure VMs only!
# Need to update the link in the azure_vm_init_script.sh with your commit hash to update this config
c.ServerApp.quit_button=False
c.ServerApp.certfile=''
c.ServerApp.keyfile=''
c.ServerApp.port=8888
c.ServerApp.token=''
c.ServerApp.ip=''
c.ServerApp.allow_origin="*"
c.ServerApp.disable_check_xsrf=True # to prevent 'xsrf missing from POST' error https://broadworkbench.atlassian.net/browse/IA-4284
# c.ServerApp.contents_manager_class=jupyter_delocalize.WelderContentsManager
