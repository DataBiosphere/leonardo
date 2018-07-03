import React from 'react';

import Grid from '@material-ui/core/Grid';

import ClusterCard from './ClusterCard' 


/**
 * Manages rendering of a list of cluster cards.
 * @props errorHandler function callback displays a string as a dismissable error.
 * @props googleToken string access token provided by oauth login.
 * @props oauthClientId string oauth client used for app auth.
 * @props clusterModels list of cluster objects, each object contains an object
 *        with a cluster model (used by the card component).
 */
class ClusterCardList extends React.Component {
  render() {
    var clusterCards = [];
    var models = this.props.clusterModels;
    for (var i = 0; i < models.length; i++) {
      var model = models[i];
      // Any difference in status should trigger card re-render.
      var clusterKey = model.googleProject + "/" + model.clusterName + "/" + model.status;
      clusterCards.push(
        <ClusterCard
          oauthClientId={this.props.oauthClientId}
          googleAuthToken={this.props.googleAuthToken}
          errorHandler={this.props.errorHandler}
          clusterModel={model}
          key={clusterKey} />
      );
    }
    return (
      <div style={{"padding": 24}}>
        <Grid
          container
          spacing={24}
          direction="column"
        >
        {clusterCards}
        </Grid>
      </div>
    );
  }
}

export default ClusterCardList;
