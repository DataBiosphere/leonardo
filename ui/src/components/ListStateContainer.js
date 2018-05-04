import React from 'react';
import CssBaseline from 'material-ui/CssBaseline';

import ClusterCardList from './ClusterCardList';
import CreateClusterModalButton from './CreateClusterModalButton';
import LeonardoAppBar from './LeonardoAppBar';


/**
 * Contains and manages the list of user-accessible notebook clusters.
 * @props errorHandler function callback displays a string as a dismissable error.
 * @props googleProfile googleUser.profileObj from gapi, describes logged in user.
 * @props googleToken string access token provided by oauth login.
 * @props oauthClientId string oauth client used for app auth.
 */
class ListStateContainer extends React.Component {

  /**
   * Set state defaults & ensure the environment's correct refresh callback is used.
   */
  constructor(props) {
    super(props);
    var refreshCallback = this.refreshListFromApi;
    var filterUser = true;
    if (process.env.REACT_APP_DEPLOY_ENVIRONMENT === "local") {
      refreshCallback = this.fakeListClustersCallback
    }
    if (process.env.REACT_APP_FILTER_TO_CURRENT_USER !== "true") {
      filterUser = false;
    }
    this.state = {
      clusters: [],
      clusterListRefreshCallback: refreshCallback,
      perUserFilter: filterUser
    };
  }

  /**
   * Called exactly once per instance, prior to the initial render of the component
   * this stars a cluster list state update so that the initial list of cluster
   * cards is shown.
   */
  componentWillMount() {
    this.state.clusterListRefreshCallback();
  }

  /**
   * Refreshes state.clusters state by calling leonardos list API. Current 
   */
  refreshListFromApi = () => {
    // Construct the request URI.
    var listUri = "/api/clusters?includeDeleted=false";
    if (this.state.perUserFilter) {
      var creatorFilter = encodeURIComponent("creator=" + this.props.googleProfile.email);
      listUri = listUri + "&" + "_labels=" + creatorFilter;
    }
    // Begin the GET request and register callbacks.
    return fetch(
      listUri,
      {
        method: "GET",
        headers: {
          "Authorization": "Bearer " + this.props.googleAuthToken
        },
        credentials: "include"
      }
    )
    // Validate response content type is application/json.
    .then((response) => {
      if (response.headers.get("content-type").indexOf("application/json") <= -1) {
        console.log(response);
        throw new Error("Fetch failed: content must be application/json");
      }
      return response
    })
    // Validate response status and throw if not in 200s.
    .then((response) => {
      if (response.status < 200 || response.status >= 300) {
        console.log(response);
        throw new Error("Fetch failed: status=" + response.status.toString());
      }
      return response;
    })
    // Get response json object.
    .then((response) => response.json())
    // Update object state.
    .then((listJson) => this.setState({clusters: listJson}))
    // Handle any errors.
    .catch((error) => this.props.errorHandler(error.toString()));
  }

  /**
   * Refreshes state.clusters with a fake response, usefull for UI development.
   */
  fakeListClustersCallback = () => {
    var fakeClusters = [{
      "machineConfig": {
          "numberOfWorkers": 0,
          "masterMachineType": "n1-standard-2"},
      "creator": "someuser@example.com",
      "googleProject": "fake-gcp-project",
      "status": "Running",
      "clusterUrl": "https://leonardo.example.com/notebooks/fake-gcp-project/my-cluster",
      "clusterName": "my-cluster",
      "createdDate": "2018-03-02T15:57:07.550Z"},
      {"machineConfig": {
          "numberOfWorkers": 3,
          "masterMachineType": "n1-standard-8"},
      "creator": "someuser@example.com",
      "googleProject": "fakeproject",
      "status": "Creating",
      "clusterUrl": "https://leonardo.example.com/notebooks/fakeproject/new-cluster",
      "clusterName": "new-cluster",
      "createdDate": "2018-03-20T09:30:00.000Z"}
    ];
    this.setState({clusters: fakeClusters});
  }

  render() {
    return (
      <div>
        <CssBaseline />
        <div>
          <LeonardoAppBar
            cardsRefreshHandler={this.state.clusterListRefreshCallback}
          />
          <ClusterCardList
            googleAuthToken={this.props.googleAuthToken}
            oauthClientId={this.props.oauthClientId}
            errorHandler={this.props.errorHandler}
            clusterModels={this.state.clusters}
          />
        </div>
        <CreateClusterModalButton
          googleAuthToken={this.props.googleAuthToken}
          cardsRefreshHandler={this.state.clusterListRefreshCallback}
          errorHandler={this.props.errorHandler}
        />
      </div>
    );
  }
}

export default ListStateContainer;
