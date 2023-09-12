
from cloneHelper import *
import os
import threading
import time


# INSTRUCTIONS:
# execute `export AZURE_TOKEN=...` with your active azure token.
# same for bee name and billing project name
azure_token = os.environ.get("AZURE_TOKEN")
bee_name = os.environ.get("BEE_NAME")
billing_project_name = os.environ.get("BILLING_PROJECT_NAME")

# Update these values as desired
number_of_clones=10 #number of clones to make
#if false, will wait for each api clone call to complete before starting the next
#if true, will start all api calls at once on different threads
parallel=True
#name of workspace to clone
workspace_name="api-workspace-ylphf"

workspace_manager_url, rawls_url, leo_url = setup(bee_name)
header = {"Authorization": "Bearer " + azure_token};

cloned_workspaces = []
if parallel:
    results_lock = threading.Lock()

    # Create a thread function that performs the task and appends the result to the list
    def clone_and_append_result(billing_project_name, workspace_name, header):
        result = clone_workspace(billing_project_name, workspace_name, header)

        # Use the lock to safely append the result to the list
        with results_lock:
            cloned_workspaces.append(result)

    proc = []
    for i in range(number_of_clones):
        t = threading.Thread(target=clone_and_append_result, args=(billing_project_name, workspace_name, header))
        proc.append(t)
        time.sleep(0.1)
        t.start()
    for t in proc:
        t.join()
else:
    while number_of_clones != 0:
        clone_id = clone_workspace(billing_project_name, workspace_name, header)
        cloned_workspaces.append(clone_id)
        number_of_clones-=1

print("Sleeping...")
time.sleep(200)
for ws in cloned_workspaces:
    wds_url = get_app_url(ws, "wds", azure_token)
    check_wds_data(wds_url, ws, "test", azure_token)

print("LOAD TEST COMPLETE.")