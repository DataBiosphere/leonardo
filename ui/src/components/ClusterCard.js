import React from 'react';

import Card, { CardActions, CardContent } from 'material-ui/Card';
import Grid from 'material-ui/Grid';
import PropTypes from 'prop-types';
import Typography from 'material-ui/Typography';
import { withStyles } from 'material-ui/styles';

import ConnectButton from './ConnectButton'
import DeleteDialog from './DeleteDialog'
import StatusIcon from './StatusIcon'


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
 * @props googleToken string access token provided by oauth login.
 * @props oauthClientId string oauth client used for app auth.
 * @props clusterModel object contains a cluster model returned by Leonardo's API.
 */
class ClusterCard extends React.Component {

  constructor(props) {
    super(props);
    this.state = {
      clusterStatus: this.props.clusterModel.status,
    };
  }

  setClusterStatusRunning = () => {
    this.setState({ clusterStatus: "Running" });
  }

  setClusterStatusError = () => {
    this.setState({ clusterStatus: "Error" });
  }

  setClusterStatusDeleting = () => {
    this.setState({ clusterStatus: "Deleting" });
  }

  render() {
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
