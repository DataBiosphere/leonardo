import React from 'react';

import Card, { CardActions, CardContent } from 'material-ui/Card';
import Grid from 'material-ui/Grid';
import PropTypes from 'prop-types';
import Typography from 'material-ui/Typography';
import { withStyles } from 'material-ui/styles';

import ConnectButton from './ConnectButton'
import DeleteDialog from './DeleteDialog'
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


const statusCheckInterval = 30000;  // 30 seconds.
const maxStatusChecks = 30; // 30 checks, or, 15 minutes.


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
      statusFetches: 0
    };
  }

  setClusterStatusDeleting = () => {
    this.setState({ clusterStatus: "Deleting" });
  }

  /**
   * Refresh cluster status if there have been fewer than `maxStatusChecks` prior refreshes.
   */
  tryRefreshingClusterStatus = () => {
    // Exit early if too many checks have been done. Set status to unknown.
    if (this.state.statusFetches > maxStatusChecks) {
      this.setState({clusterStatus: unknownStatus});
      return
    }
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
    // Validate response content type is application/json.
    .then((response) => {
      if (response.headers.get("content-type").indexOf("application/json") <= -1) {
        console.log("List cluster response not of type 'application/json'")
        console.log(response);
        this.setState({clusterStatus: unknownStatus, statusFetches: 0});
      }
      return response
    })
    // Validate response status and throw if not in 200s.
    .then((response) => {
      if (response.status < 200 || response.status >= 300) {
        console.log(response);
        this.setState({clusterStatus: unknownStatus, statusFetches: 0});
      }
      return response;
    })
    // Get response json object.
    .then((response) => response.json())
    // Update the status only if the status is new, otherwise increment the counter.
    .then((responseJson) => {
      if (responseJson.status !== this.state.clusterStatus) {
        this.setState({clusterStatus: responseJson.status, statusFetches: 0});
      } else {
        this.setState({statusFetches: this.state.statusFetches + 1});
      }
    })
    // Handle any errors.
    .catch((error) => this.props.errorHandler(error.toString()));
  }

  render() {
    if (statusInFlux.indexOf(this.state.clusterStatus) >= 0) {
      setTimeout(this.tryRefreshingClusterStatus, statusCheckInterval);
    }

    // Grab variables used for rendering.
    var classes = this.props.classes;
    var model = this.props.clusterModel;
    var machineCfg = model.machineConfig;
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
        <ConnectButton
          oauthClientId={this.props.oauthClientId}
          errorHandler={this.props.errorHandler}
          googleAuthToken={this.props.googleAuthToken}
          clusterStatus={this.state.clusterStatus}
          clusterModel={model}
        />
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
