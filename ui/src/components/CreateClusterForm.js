import React from 'react';
import PropTypes from 'prop-types';
import { withStyles } from 'material-ui/styles';
import { InputLabel } from 'material-ui/Input';
import TextField from 'material-ui/TextField';
import Typography from 'material-ui/Typography';
import { MenuItem } from 'material-ui/Menu';
import { FormControl } from 'material-ui/Form';
import Select from 'material-ui/Select';
import Button from 'material-ui/Button';
import { CircularProgress } from 'material-ui/Progress';

import { createApiUrl } from '../net'


const DEFAULT_MASTER_MACHINE_TYPE = "n1-standard-4";
const DEFAULT_WORKER_MACHINE_TYPE = "n1-standard-2";
const DEFAULT_WORKER_DISK_SIZE = 200;

// Regex for GCP Project names.
const projectRegex = /^[a-z][a-z0-9_-]{0,50}[a-z0-9]$/;

// Regex for cluster names.
const clusterRegex = /^[a-z][a-z0-9-]{0,25}[a-z0-9]$/;

const validMachineTypes = [
  "n1-standard-2",
  "n1-standard-4",
  "n1-standard-8"];

const validDiskSizes = [200, 500, 1000];

/**
 * Spinner that only shows when there's a request in progress.
 * @props requestInProgress bool indicates request status.
 */
class HiddenSpinner extends React.Component {
  render() {
    if (this.props.requestInProgress) {
      return (
        <span>
          <CircularProgress color="secondary" />
        </span>);
    } else {
      return <span/>
    }
  }
}


/**
 * Render a form used to configure and launch a new cluster.
 * @props closeFormOnSuccess function callback to modal to close the
 *        form and refresh clusters from the API.
 * @props closeFormOnFailure function callback to modal to close the
 *        form and display the error.
 * @props googleAuthToken string auth token from oauth login.
 */
class UnstyledCreateClusterForm extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      clusterName: "",
      googleProject: "",
      machineType: DEFAULT_MASTER_MACHINE_TYPE,
      workerMachineType: DEFAULT_WORKER_MACHINE_TYPE,
      diskSize: 200,
      createRequestValid: false,
      requestInProgress: false,
      numberOfWorkers: 0,
    };
  }

  postStateChangeValidationCallback = () => {
    if (projectRegex.test(this.state.googleProject)
        && clusterRegex.test(this.state.clusterName)
        && validMachineTypes.includes(this.state.machineType)
        && validDiskSizes.includes(this.state.diskSize)) {
      this.setState({createRequestValid: true});
    } else {
      this.setState({createRequestValid: false});
    }
  }

  createClusterRequest = (event) => {
    // Sets state to disable submission & and show loading spinner.
    this.setState({
      createRequestValid: false,
      requestInProgress: true,
    });

    var createRequest = {
      labels: {},
      machineConfig: {
        // Worker config.
        numberOfWorkers: this.state.numberOfWorkers,
        workerMachineType: this.state.workerMachineType,
        numberOfPreemptibleWorkers: 0,
        workerDiskSize: DEFAULT_WORKER_DISK_SIZE,
        // Master config
        masterDiskSize: this.state.diskSize,
        masterMachineType: this.state.machineType

      }
    };
    // Use fetch to send a put request, and register success/fail callbacks.
    fetch(
      createApiUrl(this.state.googleProject, this.state.clusterName),
      {
        body: JSON.stringify(createRequest),
        method: "PUT",
        headers: {
          "Authorization": "Bearer " + this.props.googleAuthToken,
          "content-type": "application/json"
        },
        credentials: "include"
      })
      // Check for responses indicating failure. If a reason was given, try
      // parsing that reason and giving it to the user via the thrown error.
      .then((response) => {
        if (response.status < 200 || response.status >= 300) {
          console.log(response);
          if (response.headers.get("content-type").includes("application/json")) {
            throw new Error(response.json().reason);
          } else {
            throw new Error("Create failed with status " + response.status.toString());
          }
        }
        return response;
      })
      .then((response) => response.json())
      // On success, close the form. Callback will also refresh the cluster list.
      .then((responseJson) => this.props.closeFormOnSuccess())
      // Handle errors.
      .catch((error) => {
        this.props.closeFormOnFailure(error.toString());
      });
  }

  handleChange = name => event => {
    this.setState(
      {[name]: event.target.value},
      this.postStateChangeValidationCallback);
  };

  render() {
    const { classes } = this.props;
    return (
      <form noValidate autoComplete="off">
      {/* Google cloud project entry */}
        <div>
          <FormControl className={classes.wideFormControl}>
            <TextField
              id="googleProject"
              label="Google Project"
              value={this.state.googleProject}
              onChange={this.handleChange("googleProject")}
              margin="normal"
            />
          </FormControl>
        </div>
      {/* Cluster name. */}
        <div>
          <FormControl className={classes.wideFormControl}>
            <TextField
              id="clusterName"
              label="Cluster Name"
              value={this.state.clusterName}
              onChange={this.handleChange("clusterName")}
              margin="normal"
            />
          </FormControl>
        </div>
        <div style={{marginTop: 25}}>
      {/* Configure master node type and storage. */}
          <Typography
            className={classes.formInfo}
            color="inherit"
          >
            Master node configuration:
          </Typography>
          <FormControl className={classes.formControl}>
            <InputLabel htmlFor="name-machine-type">Machine type</InputLabel>
            <Select
              label="Machine Type"
              value={this.state.machineType}
              onChange={this.handleChange("machineType")}>
              <MenuItem value={"n1-standard-2"}>2 cores, 7.5 GB</MenuItem>
              <MenuItem value={"n1-standard-4"}>4 cores, 15 GB</MenuItem>
              <MenuItem value={"n1-standard-8"}>8 cores, 30 GB</MenuItem>
            </Select>
          </FormControl>
          <FormControl className={classes.mediumFormControl}>
            <InputLabel htmlFor="disk-size">Disk Size</InputLabel>
            <Select
              label="Disk Size"
              value={this.state.diskSize}
              onChange={this.handleChange("diskSize")}>
              <MenuItem value={200}>200 GB</MenuItem>
              <MenuItem value={1000}>1000 GB</MenuItem>
            </Select>
          </FormControl>
        </div>
        <div style={{marginTop: 25}}>
      {/* Configure worker node number and type. */}
          <Typography
            className={classes.formInfo}
            color="inherit"
          >
            Cluster worker configuration:
          </Typography>
          <FormControl className={classes.narrowFormControl}>
            <InputLabel htmlFor="number-of-workers">Workers</InputLabel>
            <Select
              label="Workers"
              value={this.state.numberOfWorkers}
              onChange={this.handleChange("numberOfWorkers")}
            >
              <MenuItem value={0}>0</MenuItem>
              <MenuItem value={2}>2</MenuItem>
              <MenuItem value={4}>4</MenuItem>
              <MenuItem value={8}>8</MenuItem>
            </Select>
          </FormControl>
          <FormControl
            className={classes.formControl}
            disabled={this.state.numberOfWorkers < 1}
          >
            <InputLabel htmlFor="worker-machine-type">Worker machine type</InputLabel>
            <Select
              label="Worker type"
              value={this.state.workerMachineType}
              onChange={this.handleChange("workerMachineType")}
            >
              <MenuItem value={"n1-standard-2"}>2 cores, 7.5 GB</MenuItem>
              <MenuItem value={"n1-standard-4"}>4 cores, 15 GB</MenuItem>
              <MenuItem value={"n1-standard-8"}>8 cores, 30 GB</MenuItem>
            </Select>
          </FormControl>
        </div>
        <div style={{marginTop: 25}}>
      {/* Create the cluster. */}
        <Button
          disabled={!this.state.createRequestValid}
          variant="raised"
          color="primary"
          onClick={this.createClusterRequest}
        >
          create
        </Button>
        <HiddenSpinner
          requestInProgress={this.state.requestInProgress} />
        </div>
      </form>
    );
  }
}

UnstyledCreateClusterForm.propTypes = {
  classes: PropTypes.object.isRequired,
};


const formSyles = theme => ({
  wideFormControl: {
    margin: theme.spacing.unit,
    minWidth: 250,
  },
  formControl: {
    margin: theme.spacing.unit,
    minWidth: 180,
  },
  mediumFormControl: {
    margin: theme.spacing.unit,
    minWidth: 100,
  },
  narrowFormControl: {
    margin: theme.spacing.unit,
    minWidth: 40,
  },
  formInfo: {
    fontSize: 18,
    marginTop: 20,
    marginBottom: 10,
  }
});

export default withStyles(formSyles)(UnstyledCreateClusterForm);
