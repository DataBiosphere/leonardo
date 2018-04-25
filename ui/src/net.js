/* Utility functions for networking. */


export function createNotebooksUrl(googleProject, instanceName) {
  var encName = encodeURIComponent(instanceName);
  var encProject = encodeURIComponent(googleProject);
  return "/notebooks/" + encProject + "/" + encName;
}


export function createApiUrl(googleProject, instanceName) {
  var encName = encodeURIComponent(instanceName);
  var encProject = encodeURIComponent(googleProject);
  return "/api/cluster/" + encProject + "/" + encName;
}
