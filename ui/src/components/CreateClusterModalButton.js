import React from 'react';
import PropTypes from 'prop-types';

import Icon from 'material-ui/Icon';
import IconButton from 'material-ui/IconButton';
import Modal from 'material-ui/Modal';
import { withStyles } from 'material-ui/styles';
import Typography from 'material-ui/Typography';
import Tooltip from 'material-ui/Tooltip';

import CreateClusterForm from './CreateClusterForm'

const styles = theme => ({
  paper: {
    position: 'absolute',
    width: theme.spacing.unit * 70,
    backgroundColor: theme.palette.background.paper,
    boxShadow: theme.shadows[5],
    padding: theme.spacing.unit * 4,
  },
  button: {
    fontSize: 38,
    '&:hover': {textShadow: "-1.5px 1.5px #B2BCC1"}
  }
});

const modalPosition = {
    top: "50%",
    left: "50%",
    transform: "translate(-50%, -50%)",
};


/**
 * Button that opens a modal for cluster creation.
 * @props googleAuthToken string auth token from oauth login.
 * @props cardsRefreshHandler function callback to refresh cluster list state.
 * @props errorHandler function callback to display errors text.
 */
class CreateClusterModalButton extends React.Component {
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

  closeFormOnSuccess = () => {
    this.handleClose();
    this.props.cardsRefreshHandler();
  }

  closeFormOnFailure = (failReason) => {
    this.handleClose();
    this.props.errorHandler(failReason);
  }

  render() {
    return (
      <div style={{paddingLeft:24, paddingBottom:32}}>
        <IconButton onClick={this.handleClickOpen}>
          <Tooltip
          id="tooltip-right"
          placement="right"
          title="Create a notebook server"
          >
            <Icon color="secondary" className={this.props.classes.button}>
              add_circle
            </Icon>
          </Tooltip>
        </IconButton>
        <Modal
          aria-labelledby="simple-modal-title"
          aria-describedby="simple-modal-description"
          open={this.state.open}
          onClose={this.handleClose}
        >
          <div style={modalPosition} className={this.props.classes.paper}>
            <Typography variant="title" id="modal-title">
              Create a cluster:
            </Typography>
            <CreateClusterForm
              googleAuthToken={this.props.googleAuthToken}
              closeFormOnSuccess={this.closeFormOnSuccess}
              closeFormOnFailure={this.closeFormOnFailure} />
        </div>
      </Modal>
      </div>
    );
  }
}

CreateClusterModalButton.propTypes = {
  classes: PropTypes.object.isRequired,
};


export default withStyles(styles)(CreateClusterModalButton);;
