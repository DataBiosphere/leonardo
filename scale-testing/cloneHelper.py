
import json
import random
import string
import wds_client
import requests

workspace_manager_url = ""
rawls_url = ""
leo_url = ""

def setup(env):
# define major service endpoints based on bee name
    global workspace_manager_url
    workspace_manager_url = f"https://workspace.dsde-{env}.broadinstitute.org"
    global rawls_url
    rawls_url = f"https://rawls.dsde-{env}.broadinstitute.org"
    global leo_url
    leo_url = f"https://leonardo.dsde-{env}.broadinstitute.org"
    return workspace_manager_url, rawls_url, leo_url

# GET WDS OR CROMWELL ENDPOINT URL FROM LEO
def get_app_url(workspaceId, app, azure_token):
    """"Get url for wds/cbas."""
    uri = f"{leo_url}/api/apps/v2/{workspaceId}?includeDeleted=false"
    # print(uri)
    headers = {"Authorization": "Bearer " + azure_token,
               "accept": "application/json"}

    response = requests.get(uri, headers=headers)
    # print(response)
    status_code = response.status_code

    if status_code != 200:
        return response.text
    print(f"Successfully retrieved details.")
    response = json.loads(response.text)

    app_url = ""
    app_type = "CROMWELL" if app != 'wds' else app.upper();
    # print(f"App type: {app_type}")
    for entries in response: 
        if entries['appType'] == app_type and entries['proxyUrls'][app] is not None:
            # print(entries['status'])
            if(entries['status'] == "PROVISIONING"):
                print(f"{app} is still provisioning")
                break
            # print(f"App status: {entries['status']}")
            app_url = entries['proxyUrls'][app]
            break 

    if app_url is None: 
        print(f"{app} is missing in current workspace")
    else:
        print(f"{app} url: {app_url}")

    return app_url

def clone_workspace(source_billing_project_name, dest_billing_project_name, workspace_name, header):
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
    return json2["workspaceId"]


def check_wds_data(wds_url, workspaceId, recordName, azure_token):
    version = "v0.2"
    api_client = wds_client.ApiClient(header_name='Authorization', header_value="Bearer " + azure_token)
    api_client.configuration.host = wds_url

    schema_client = wds_client.SchemaApi(api_client)

    print("verifying data was cloned")
    response = schema_client.describe_record_type(workspaceId, version, recordName);
    assert response.name == 'student', "Name does not match"
    assert response.count == 86, "Count does not match"
