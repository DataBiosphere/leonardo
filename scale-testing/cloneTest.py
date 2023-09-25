
from cloneHelper import *
import os
import threading
import time


# INSTRUCTIONS:
# execute `export AZURE_TOKEN=...` with your active azure token.
# same for bee name and billing project name
azure_token = os.environ.get("AZURE_TOKEN")
#bee_name = os.environ.get("BEE_NAME")
env = os.environ.get("ENV")
source_billing_project_name = os.environ.get("SOURCE_BILLING_PROJECT_NAME")
dest_billing_project_name = os.environ.get("DEST_BILLING_PROJECT_NAME")
workspace_name = os.environ.get("WORKSPACE_NAME")

# Update these values as desired
number_of_clones=1 #number of clones to make
#if false, will wait for each api clone call to complete before starting the next
#if true, will start all api calls at once on different threads
parallel=True
run_workflow=True

workspace_manager_url, rawls_url, leo_url = setup(env)
header = {"Authorization": "Bearer " + azure_token};

cloned_workspaces = []
if parallel:
    results_lock = threading.Lock()

    # Create a thread function that performs the task and appends the result to the list
    def clone_and_append_result(source_billing_project_name, dest_billing_project_name, workspace_name, header):
        result = clone_workspace(source_billing_project_name, dest_billing_project_name, workspace_name, header)

        # Use the lock to safely append the result to the list
        with results_lock:
            cloned_workspaces.append(result)

    proc = []
    for i in range(number_of_clones):
        t = threading.Thread(target=clone_and_append_result, args=(source_billing_project_name, dest_billing_project_name, workspace_name, header))
        proc.append(t)
        time.sleep(0.1)
        t.start()
    for t in proc:
        t.join()
else:
    while number_of_clones != 0:
        clone_id = clone_workspace(source_billing_project_name, dest_billing_project_name, workspace_name, header)
        cloned_workspaces.append(clone_id)
        number_of_clones-=1

print("Sleeping...")
time.sleep(60)
for ws in cloned_workspaces:
    # check WDS data was cloned
    wds_url = poll_for_app_url(ws, "wds", azure_token)
    check_wds_data(wds_url, ws, "student", azure_token)

    if run_workflow:
        # next trigger a workflow in each of the workspaces, at this time this doesnt monitor if this was succesful or not
        start_cbas(ws, header)
        method_id = add_workflow_method(ws, azure_token)
        submit_workflow(ws, "resources/calculate_gpa_run.json", azure_token, method_id)


print("LOAD TEST COMPLETE.")