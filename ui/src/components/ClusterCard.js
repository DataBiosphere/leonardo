import React from 'react';

import Card from '@material-ui/core/Card';
import CardActions from '@material-ui/core/CardActions';
import CardContent from '@material-ui/core/CardContent';
import Grid from '@material-ui/core/Grid';
import PropTypes from 'prop-types';
import Typography from '@material-ui/core/Typography';
import { withStyles } from '@material-ui/core/styles';

import ConnectButton from './ConnectButton'
import DeleteDialog from './DeleteDialog'
import StartButton from './StartButton'
import StatusIcon from './StatusIcon'


// The following statuses indicate that the status
// of the cluster is likely to change in the near
// future.
const statusInFlux = [
  "Creating",
  "Starting",
  "Stopping",
  "Updating",
  "Deleting",
]

const unknownStatus = "Unknown";
const runningStatus = "Running";
const deletingStatus = "Deleting";
const deletedStatus = "Deleted";
const stoppedStatus = "Stopped";
const errorString404 = "404/Not Found";


const slowStatusCheckInterval = 5*60000;  // Refresh every five minutes for running clusters.
const fastStatusCheckInterval = 30000;  // 30 seconds.
const maxFastStatusChecks = 30; // 30 checks, or, 15 minutes.


const styles = {
  card: {
    minWidth: 300,
    maxWidth: 550,
  },
  supertitle: {
    marginBottom: 0,
    marginTop: 0,
    fontSize: 16
  },
  title: {
    marginBottom: 10,
    fontSize: 24
  },
  subtitle: {
    marginBottom: 12,
  },
  description: {
    fontSize: 15,
  },
};


/**
 * @props errorHandler function callback displays a string as a dismissable error.
 * @props googleAuthToken string access token provided by oauth login.
 * @props oauthClientId string oauth client used for app auth.
 * @props clusterModel object contains a cluster model returned by Leonardo's API.
 */
class ClusterCard extends React.Component {

  constructor(props) {
    super(props);
    this.state = {
      clusterStatus: this.props.clusterModel.status,
    };
    // This tracks internal state and should not be attached to "this.state" since
    // updates to the component state will trigger rendering.
    this.fastStatusRefreshes = 0
  }

  setClusterStatusDeleting = () => {
    this.setState({ clusterStatus: "Deleting" });
    setTimeout(this.checkForUpdatedState, fastStatusCheckInterval)
  }

  setClusterStatusStarting = () => {
    this.setState({ clusterStatus: "Starting" });
    setTimeout(this.checkForUpdatedState, fastStatusCheckInterval)
  }

  /**
   * Manage timing of cluster state re-fetches.
   * This function will refresh the cluster status every ~5 minutes for running clusters
   * and every ~30 seconds for clusters whose state is currently changing. For clusters
   * that are "in flux", the refreshes will eventually give up and set the status to
   * unknown.
   */
  checkForUpdatedState = () => {
    // Default actions; do nothing.
    var nextRefreshTimeout = 0;  // Timeout for next refresh cycle, 0 == don't refresh again.
    var refreshFromService = false;  // Should we check the API this cycle?
    var stateInFlux = (statusInFlux.indexOf(this.state.clusterStatus) >= 0);
    // Use rapid status-updates up to 30 times when state.clusterStatus is in flux.
    if (stateInFlux && this.fastStatusRefreshes < maxFastStatusChecks) {
      nextRefreshTimeout = fastStatusCheckInterval
      refreshFromService = true
      this.fastStatusRefreshes += 1
    // If the fast checks have been expended, set state to unknown and give up.
    } else if (stateInFlux && this.fastStatusRefreshes >= maxFastStatusChecks) {
      console.log("Could not determine cluster state after " + maxFastStatusChecks + " attempts.")
      this.setState({clusterStatus: unknownStatus})
      refreshFromService = false
    // When state is 'running' re-check state slowly.
    } else if (this.state.clusterStatus === runningStatus) {
      nextRefreshTimeout = slowStatusCheckInterval;
      refreshFromService = true;
    }

    // Hit the API if needed.
    if (refreshFromService) {
      this.refreshStateFromAPI()
    }
    // Set timeout to run this again. Random fuzz the timeout by +/- 5%.
    if (nextRefreshTimeout > 0) {
      nextRefreshTimeout += Math.round(nextRefreshTimeout * ((Math.random() * 0.1) - 0.05))
      setTimeout(this.checkForUpdatedState, nextRefreshTimeout)
    }
  }

  /**
   * Refresh cluster status from the Leonardo get-cluster API.
   */
  refreshStateFromAPI = () => {
    console.log("starting refresh")
    // Path to fetch cluster json.
    var getPath = '/api/cluster/' + this.props.clusterModel.googleProject + '/' + this.props.clusterModel.clusterName;
    // Begin the GET request and register callbacks.
    return fetch(
      getPath,
      {
        method: "GET",
        headers: {
          "Authorization": "Bearer " + this.props.googleAuthToken
        },
        credentials: "include"
      }
    )
    // Validate response is OK.
    .then((response) => {
      if (response.status == 404) {
        return Promise.reject(errorString404)
      }
      if (response.status < 200 || response.status >= 300) {
        console.log(response);
        // Rejected promise jumps to "catch" clause.
        return Promise.reject("Response status not OK")
      }
      console.log("updating after code.")
      return response;
    })
    // Validate response content type is application/json.
    .then((response) => {
      var contentType = (response.headers.get("content-type"));
      if (contentType.indexOf("application/json") <= -1) {
        console.log(response);
        return Promise.reject("Content type must be json, not \"" + contentType + "\"")
      }
      return response
    })
    // Get get response json promise.
    .then((response) => response.json())
    // Update the status only if the status is new.
    .then((responseJson) => {
      if (responseJson.status !== this.state.clusterStatus) {
        // In time a new status is found from the API reset the 'fast' counter.
        this.fastStatusRefreshes = 0;
        this.setState({clusterStatus: responseJson.status});
      }
    })
    // Handle any errors without killing the page or bothering the user.
    .catch((error) => {
      // Once a "Deleting" cluster is deleted, expect 404 and update the card accordingly.
      if (this.state.clusterStatus === deletingStatus && error === errorString404) {
        this.setState({clusterStatus: deletedStatus})
        return
      }
      console.log("Error while updating status for " + this.props.clusterModel.clusterName);
      console.log(error);
      this.setState({clusterStatus: unknownStatus});
    });
  }

  componentWillMount() {
    this.checkForUpdatedState()
  }

  render() {
    // Grab variables used for rendering.
    var classes = this.props.classes;
    var model = this.props.clusterModel;
    var machineCfg = model.machineConfig;
    var startOrConnect = null;
    if (this.state.clusterStatus === stoppedStatus) {
      startOrConnect = (<StartButton
            errorHandler={this.props.errorHandler}
            googleAuthToken={this.props.googleAuthToken}
            updateStatusToStarting={this.setClusterStatusStarting}
            clusterModel={model}
          />
      );
    } else {
      startOrConnect = (<ConnectButton
            oauthClientId={this.props.oauthClientId}
            errorHandler={this.props.errorHandler}
            googleAuthToken={this.props.googleAuthToken}
            clusterStatus={this.state.clusterStatus}
            clusterModel={model}
          />
      );
    }
    return (
      <Grid item xs={12} sm={10}>
      <Card className={classes.card}>
      <CardContent>
        <Grid container justify="space-between">
          <Grid item xs={10}>
            <Typography
              className={classes.supertitle}
              color="textSecondary"
            >
              {model.googleProject}
            </Typography>
            <Typography
              className={classes.title}>
              {model.clusterName}
            </Typography>
          </Grid>
          <Grid
            item
            xs={1}
          >
            <StatusIcon clusterStatus={this.state.clusterStatus} />
          </Grid>
        </Grid>

        <Typography
          xs={8}
          className={classes.description}
        >
          Owner: {model.creator}
          <br/>
          Master type: {machineCfg.masterMachineType}
          <br/>
          Workers: {machineCfg.numberOfWorkers}
        </Typography>
      </CardContent>
      <CardActions>
        {startOrConnect}
        <DeleteDialog
          clusterStatus={this.state.clusterStatus}
          errorHandler={this.props.errorHandler}
          cardClusterDeleteCallback={this.setClusterStatusDeleting}
          googleAuthToken={this.props.googleAuthToken}
          clusterModel={model}
        />
      </CardActions>
      </Card>
      </Grid>
    );
  }
}

ClusterCard.propTypes = {
  classes: PropTypes.object.isRequired,
};


export default withStyles(styles)(ClusterCard);
