import React from 'react';

import GoogleSignInWrapper from './GoogleSignInWrapper'

/**
 * Load the config, blocking rendering of the application until loading is complete.
 * @props errorHandler function callback displays a string as a dismissable error.
 */
class ConfigLoader extends React.Component {

  constructor(props) {
    super(props);
    this.state = {configLoading: true};
  }

  /**
   * Load configuration from static assets.
   */
  loadConfig = () => {
    fetch("/config.json")
      .then((response) => {
        if (response.status < 200 || response.status >= 300) {
          throw new Error("Failed to load config.json with status " + response.status.toString());
        }
        return response;
      })
      .then((response) => response.json())
      // On success, set the global config value and update the loading status.
      .then((responseJson) => {
        window.GlobalReactConfig = responseJson;
        this.setState({configLoading: false});
      })
      // Handle errors.
      .catch((error) => {
        this.props.errorHandler("Failed to load config.");
      });
  }

  render() {
    // Load the config before showing sign-in modal.
    if (this.state.configLoading) {
      this.loadConfig()
      return <div>Loading configuration</div>
    }
    return (<GoogleSignInWrapper errorHandler={this.props.errorHandler} />)
  }
}

export default ConfigLoader;