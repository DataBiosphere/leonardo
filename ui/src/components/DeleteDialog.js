import React from 'react';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';

import { createApiUrl } from '../net'


// List of status that disallow deletion.
const deletedStatus = ["Deleting", "Deleted"];


/**
 * Provide a button to delete the cluster featuring a confirmation dialog
 * to prevent accidential deletion.
 * @props clusterStatus string of cluster status.
 * @props errorHandler function callback to display errors.
 * @props cardClusterDeleteCallback function callback to update card state
          so that the cluster shows as deleting.
 * @props googleAuthToken string access token provided by oauth login.
 * @props clusterModel object with cluster information from Leonardo service.
 */
class DeleteDialog extends React.Component {

  constructor(props) {
    super(props);
    this.state = {
      open: false,
    };
  }

  handleClickOpen = () => {
    this.setState({ open: true });
  }

  handleClose = () => {
    this.setState({ open: false });
  }

  handleDeleteConfirm = () => {
    var model = this.props.clusterModel;
    fetch(
      createApiUrl(model.googleProject, model.clusterName),
      {
        method: "DELETE",
        headers: {
          "Authorization": "Bearer " + this.props.googleAuthToken,
          "content-type": "application/json"
        },
        credentials: "include"
      }
    )
    // Validate response status.
    .then((response) => {
      if (response.status < 200 || response.status >= 300) {
        console.log(response);
        throw new Error("Fetch failed: status=" + response.status.toString());
      }
      return response;
    })
    // Run card callback.
    .then((response) => {
      this.props.cardClusterDeleteCallback();
      return response;
    })
    // Handle any errors.
    .catch((error) => this.props.errorHandler(error.toString()));
    this.handleClose()
  }

  render() {
    if (deletedStatus.indexOf(this.props.clusterStatus) > -1) {
      return <Button disabled size="medium">Delete Instance</Button>;
    }
    return (
      <div>
        <Button
          size="medium" 
          onClick={this.handleClickOpen}>
          Delete Instance
        </Button>
        <Dialog
          open={this.state.open}
          onClose={this.handleClose}
          aria-labelledby="alert-dialog-title"
          aria-describedby="alert-dialog-description"
        >
          <DialogTitle id="alert-dialog-title">{"Delete Instance?"}</DialogTitle>
          <DialogContent>
            <DialogContentText id="alert-dialog-description">
              Are you sure you want to delete this instance? You cannot undo this operation.
            </DialogContentText>
          </DialogContent>
          <DialogActions>
            <Button onClick={this.handleClose} color="primary" autoFocus>
              No, don't delete!
            </Button>
            <Button onClick={this.handleDeleteConfirm} color="primary">
              Yes
            </Button>
          </DialogActions>
        </Dialog>
      </div>
    );
  }
}

export default DeleteDialog;
