import React from 'react';
import Button from '@material-ui/core/Button';

import { createApiUrl } from '../net'


/**
 * @props errorHandler function callback displays a string as a dismissable error.
 * @props googleAuthToken string access token provided by oauth login.
 * @props updateStatusToStarting callback that update's the cluster card's state.
 * @props clusterModel object contains a cluster model returned by Leonardo's API.
 */
class StartButton extends React.Component {

  handleClick = (event) => {
    var model = this.props.clusterModel;
    // Fetch auth info which includes the Google Client ID.
    var notebooksUrl = createApiUrl(
      model.googleProject, model.clusterName);
    fetch(
       notebooksUrl + "/start",
      {
        method: "POST",
        headers: {
          "Authorization": "Bearer " + this.props.googleAuthToken
        },
        credentials: "include"
      })
    .then((response) => {
      if (response.status == 409) {
        throw new Error("Cluster cannot be started.")
      }
      if (response.status == 404) {
        throw new Error("Cluster not found.")
      }
      if (response.status >= 400) {
        console.log("Unexpected response, logging response object.")
        console.log(response)
        throw new Error("Bad response from server. Cluster may not start.");
      }
      return response;
    })
    .then((response) => this.props.updateStatusToStarting())
    .catch((error) => {
      this.props.errorHandler(error.toString());
    });
  }

  render() {
    return <Button size="medium" onClick={this.handleClick}>Start</Button>;
  }
}

export default StartButton;
