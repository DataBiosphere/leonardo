
import json
import wds_client
import requests
import uuid
import random
import string
import time

workspace_manager_url = ""
rawls_url = ""
leo_url = ""

def setup(env):
# define major service endpoints based on env
    global workspace_manager_url
    global rawls_url
    global leo_url
    if env.lower() not in ["dev", "prod", "alpha", "staging"]:
        workspace_manager_url = f"https://workspace.{env}.bee.envs-terra.bio"
        rawls_url = f"https://rawls.{env}.bee.envs-terra.bio"
        leo_url = f"https://leonardo.{env}.bee.envs-terra.bio"
    else:
        workspace_manager_url = f"https://workspace.dsde-{env}.broadinstitute.org"
        rawls_url = f"https://rawls.dsde-{env}.broadinstitute.org"
        leo_url = f"https://leonardo.dsde-{env}.broadinstitute.org"
    return workspace_manager_url, rawls_url, leo_url

# GET WDS OR CROMWELL ENDPOINT URL FROM LEO
def poll_for_app_url(workspaceId, app, azure_token):
    """"Get url for wds/cbas."""
    leo_get_app_api = f"{leo_url}/api/apps/v2/{workspaceId}?includeDeleted=false"
    headers = {"Authorization": "Bearer " + azure_token,
               "accept": "application/json"}

    app_type = "CROMWELL" if app != 'wds' else app.upper();
    print(f"App type: {app_type}")

    #TODO: can this get into an infinite loop?
    while True:
        response = requests.get(leo_get_app_api, headers=headers)
        assert response.status_code == 200, f"Error fetching apps from leo: ${response.text}"
        print(f"Successfully retrieved details.")
        response = json.loads(response.text)
        print(response)

        #TODO have i covered all cases?
        for entries in response:
            if entries['appType'] == app_type:
                if entries['status'] == "PROVISIONING":
                    print(f"{app} is still provisioning")
                    time.sleep(30)
                elif entries['status'] == 'ERROR':
                    print(f"{app} is in ERROR state. Quitting.")
                    return ""
                elif app not in entries['proxyUrls'] or entries['proxyUrls'][app] is None:
                    print(f"{app} proxyUrls not found: {entries}")
                    return ""
                else:
                    return entries['proxyUrls'][app]

def clone_workspace(source_billing_project_name, dest_billing_project_name, workspace_name, header, start_cbas=False):
    api_call2 = f"{rawls_url}/api/workspaces/{source_billing_project_name}/{workspace_name}/clone";
    request_body = {
        "namespace": dest_billing_project_name,  # Billing project name
        "name": f"{workspace_name} clone-{''.join(random.choices(string.ascii_lowercase, k=5))}",  # workspace name
        "attributes": {}};

    print(f"cloning workspace {workspace_name}")
    response = requests.post(url=api_call2, json=request_body, headers=header)

    # example json that is returned by request: 'attributes': {}, 'authorizationDomain': [], 'bucketName': '', 'createdBy': 'yulialovesterra@gmail.com', 'createdDate': '2023-08-03T20:10:59.116Z', 'googleProject': '', 'isLocked': False, 'lastModified': '2023-08-03T20:10:59.116Z', 'name': 'api-workspace-1', 'namespace': 'yuliadub-test2', 'workspaceId': 'ac466322-2325-4f57-895d-fdd6c3f8c7ad', 'workspaceType': 'mc', 'workspaceVersion': 'v2'}
    json2 = response.json()
    print(json2)

    if start_cbas is True:
        print(f"Enabling CBAS for workspace {json2['workspaceId']}")
        start_cbas_api = f"{leo_url}/api/apps/v2/{json2['workspaceId']}/terra-app-{str(uuid.uuid4())}";
        request_body2 = {
            "appType": "CROMWELL"
        }

        cbas_response = requests.post(url=start_cbas_api, json=request_body2, headers=header)
        # will return 202 or error
        print(cbas_response)
    return json2["workspaceId"]


def check_wds_data(wds_url, workspaceId, recordName, azure_token):
    version = "v0.2"
    api_client = wds_client.ApiClient(header_name='Authorization', header_value="Bearer " + azure_token)
    api_client.configuration.host = wds_url

    schema_client = wds_client.SchemaApi(api_client)

    print("verifying data was cloned")
    response = schema_client.describe_record_type(workspaceId, version, recordName);
    assert response.name == recordName, "Name does not match"
    # assert response.count == 86, "Count does not match"

def submit_workflow_assemble_refbased(workspaceId, dataFile, azure_token):
    cbas_url = poll_for_app_url(workspaceId, "cbas", azure_token)
    print(cbas_url)
    #open text file in read mode
    text_file = open(dataFile, "r")
    request_body = text_file.read();
    text_file.close()

    cbas_run_sets_api = f"{cbas_url}/api/batch/v1/run_sets"

    headers = {"Authorization": "Bearer " + azure_token,
               "accept": "application/json",
               "Content-Type": "application/json"}

    response = requests.post(cbas_run_sets_api, data=request_body, headers=headers)
    # example of what it returns:
    # {
    #   "run_set_id": "cdcdc570-f6f3-4425-9404-4d70cd74ce2a",
    #   "runs": [
    #     {
    #       "run_id": "0a72f308-4931-436b-bbfe-55856f7c1a39",
    #       "state": "UNKNOWN",
    #       "errors": "null"
    #     },
    #     {
    #       "run_id": "eb400221-efd7-4e1a-90c9-952f32a10b60",
    #       "state": "UNKNOWN",
    #       "errors": "null"
    #     }
    #   ],
    #   "state": "RUNNING"
    # }
    assert response.status_code == 200, f"Run workflow request failed: {response.text}"
    data = response.json()
    errors = [run["errors"] for run in data["runs"]]
    assert all(error == "null" for error in errors), f"Error in submitting workflow: {errors}"

def upload_wds_data(wds_url, current_workspaceId, tsv_file_name, recordName, record_count, azure_token):
    version="v0.2"
    api_client = wds_client.ApiClient(header_name='Authorization', header_value="Bearer " + azure_token)
    api_client.configuration.host = wds_url

    # records client is used to interact with Records in the data table
    records_client = wds_client.RecordsApi(api_client)
    # data to upload to wds table
    print("uploading to wds")
    # Upload entity to workspace data table with name |recordName|
    response = records_client.upload_tsv(current_workspaceId, version, recordName, tsv_file_name)
    print(response)
    assert response.records_modified == record_count, f"Uploading to wds failed: {response.reason}"

