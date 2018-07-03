import React from 'react';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';

import ConfigLoader from './ConfigLoader'


/**
 * Wrap all user interactions with div containing a hidden error popup.
 * This wrapper provides the 'errorHandler' callback used when there are
 * failures or urgent notifications from contained components.
 */
class ViewErrorInfoWrapper extends React.Component {

  constructor(props) {
    super(props);
    this.state = {
      open: false,
      errorText: ""
    };
  }

  showErrorMessage = (errorText) => {
    this.setState({
      open: true,
      errorText: errorText
    });
  }

  handleClose = () => {
    this.setState({
      open: false,
      errorText: ""
    });
  }

  render() {
    return (
      <div>
        <Dialog
            open={this.state.open}
            onClose={this.handleClose}
            aria-labelledby="alert-dialog-title"
            aria-describedby="alert-dialog-description">
          <DialogTitle id="alert-dialog-title">
            Encountered error:
          </DialogTitle>
          <DialogContent>
            <DialogContentText id="alert-dialog-description">
              {this.state.errorText}
            </DialogContentText>
          </DialogContent>
          <DialogActions>
            <Button onClick={this.handleClose} color="primary" autoFocus>
              ok
            </Button>
          </DialogActions>
        </Dialog>
        <ConfigLoader errorHandler={this.showErrorMessage} />
      </div>
    );
  }
}

export default ViewErrorInfoWrapper;