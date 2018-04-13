import React from 'react';
import Button from 'material-ui/Button';
import Dialog, {
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
} from 'material-ui/Dialog';


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
        <div> 
          placeholder
          GoogleSignInWrapper errorHandler={this.showErrorMessage}
        </div>
      </div>
    );
  }
}

export default ViewErrorInfoWrapper;