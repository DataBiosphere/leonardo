import React from 'react';

import Icon from 'material-ui/Icon';
import Tooltip from 'material-ui/Tooltip';


/**
 * Display the status as a tooltip. This is a placeholder for a component whose
 * color and icon change in response to cluster status.
 * @props clusterStatus string from clusterStatus enumeration.
 */
class StatusIcon extends React.Component {
  render() {
    return (
      <div style={{paddingTop: 10}}>
      <Tooltip id="tooltip-icon-bottom" title={this.props.clusterStatus} >
      <Icon style={{fontSize: 32}}>
        info
      </Icon>
      </Tooltip>
      </div>
    );
  }
}


export default StatusIcon;
