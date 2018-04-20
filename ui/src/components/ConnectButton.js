import React from 'react';
import Button from 'material-ui/Button';

import { createNotebooksUrl } from '../net'


function getBaseUrl() {
  return window.location.protocol + "//" + window.location.hostname;
}


class ConnectButton extends React.Component {

  handleClick = (event) => {
    var model = this.props.clusterModel;
    var baseUrl = getBaseUrl();
    // Fetch auth info which includes the Google Client ID.
    var notebooksUrl = createNotebooksUrl(
      model.googleProject, model.clusterName);
    fetch(
       notebooksUrl + "/setCookie",
      {
        method: "GET",
        headers: {
          "Authorization": "Bearer " + this.props.googleAuthToken
        },
        credentials: "include"
      })
    .then((response) => {
      if (response.status >= 400) {
        throw new Error("Bad response from server");
      }
      return response;
    })
    .then((response) => {
        // on success, open the notebook content in a new tab
        var notebook = window.open(notebooksUrl, '_blank');
        // postMessage handshake with the notebook extension
        window.addEventListener("message", (e) => {
            var clientId = this.props.oauthClientId;
            if (e.origin !== baseUrl) {
                console.log("Skipping post, wrong origin.");
                return;
            }
            if (e.data.type !== 'bootstrap-auth.request') {
                console.log("Skipping event, wrong request type.");
                return;
            }
            notebook.postMessage({
                "type": "bootstrap-auth.response",
                "body": {
                    "googleClientId": clientId
                }
            }, baseUrl);
        })
    })
    .catch((error) => {
      this.props.errorHandler(error.toString());
      console.log(error);
    });
  }

  render() {
    if (this.props.clusterStatus !== "Running") {
      return <Button disabled size="medium">Connect</Button>;
    }
    return <Button size="medium" onClick={this.handleClick}>Connect</Button>;
  }
}

export default ConnectButton;
