import React from 'react';
import Dialog, { DialogContent } from 'material-ui/Dialog';
import GoogleLogin from 'react-google-login';


/**
 * Update state on successful login or credential refresh.
 * @props errorHandler function callback displays a string as a dismissable error.
 */
class GoogleSignInWrapper extends React.Component {

  constructor(props) {
    super(props);
    this.state = {
      loggedIn: false,
      googleProfile: null,
      googleUser: null,
      googleToken: null
    };
  }

  /**
   * Set a timeout to refresh the credentials and update state.
   * @param expiresAt int timestamp (ms) at which the current token expires.
   */
  setRefreshTimeout = (expiresAt) => {
    // Either 5 minutes before the deadline, or 5 minutes from now. This
    // should prevent thrashing while keeping the credentials fresh.
    const oneMin = 60 * 1000;
    var refreshDeadline =  Math.max(
      5*oneMin,
      expiresAt - Date.now() - (5*oneMin));
    console.log("Refreshing credentials in "
                + Math.floor(refreshDeadline/oneMin).toString()
                + " minutes");
    setTimeout(this.reloadAuthToken, refreshDeadline);
  }

  /**
   * Update state on successful login or credential refresh.
   * @param googleUser gapi.auth2.GoogleUser instance returned by GoogleLogin.
   */
  initialLoginSuccess = (googleUser) => {
    this.setState({
      loggedIn: googleUser.isSignedIn(),
      googleProfile: googleUser.profileObj,
      googleToken: googleUser.tokenObj.access_token,
      googleUser: googleUser
    });
    // Set a timeout to refrsh, try to execute 5 minutes early.
    this.setRefreshTimeout(googleUser.tokenObj.expires_at);
  }

  /**
   * Handle failure for initial login.
   * @param failureResponse object returned by GoogleLogin component.
   */
  initialLoginFailure = (failureResponse) => {
    console.log("Failed to get credentials during user login");
    console.log(failureResponse);
    this.props.errorHandler("Failed to login with Google.");
    this.setState({loggedIn: false}); 
  }

  /**
   * Handle successful refreshing of the GoogleUser auth response.
   * @param authResponse gapi.auth2.AuthResponse instance with token and expiration.
   */
  reloadAuthSuccess = (authResponse) => {
    // The GoogleUser is mutated in-place, this callback updates component state.
    this.setState({googleToken: authResponse.access_token});
    this.setRefreshTimeout(authResponse.expires_at);
  }

  /**
   * Handle failure when refreshing the GoogleUser auth response.
   * @param failureResponse object describes failure.
   */
  reloadAuthFailure = (failureResponse) => {
    console.log("Failed to refresh access token.");
    console.log(failureResponse);
    this.props.errorHandler(
      "Failed to refresh credentials, you may need to log in again.");
    this.setState({loggedIn: false}); 
  }


  /**
   * Initiate reloading of a GoogleUser auth response.
   */
  reloadAuthToken = () => {
    this.state.googleUser.reloadAuthResponse().then(
      this.reloadAuthSuccess,
      this.reloadAuthFailure
    );
  }

  render() {
    var application = <div></div>;
    return (
      <div>
        <Dialog
          open={!this.state.loggedIn}
          onClose={this.handleClose}
        >
          <DialogContent>
          <GoogleLogin
            clientId={process.env.REACT_APP_OAUTH_CLIENT_ID}
            buttonText="Login with Google"
            responseType={"id_token code"}
            accessType={"offline"}
            onSuccess={this.initialLoginSuccess}
            onFailure={this.initialLoginFailure}
            prompt={"select_account"}
          />
          </DialogContent>
        </Dialog>
        {application}
      </div>
    );
  }
}

export default GoogleSignInWrapper;
