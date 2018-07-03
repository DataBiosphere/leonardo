import React from 'react';

import Icon from '@material-ui/core/Icon';

import CircularProgress from '@material-ui/core/CircularProgress';
import Tooltip from '@material-ui/core/Tooltip';

import green from '@material-ui/core/colors/green';
import grey from '@material-ui/core/colors/grey';
import yellow from '@material-ui/core/colors/yellow';
import red from '@material-ui/core/colors/red';


const statusColorIconMap = {
  Creating: {iconType: "progress", iconName: null,       color: green[500]},
  Starting: {iconType: "progress", iconName: null,       color: green[500]},
  Stopping: {iconType: "progress", iconName: null,       color: yellow[800]},
  Updating: {iconType: "progress", iconName: null,       color: yellow[800]},
  Deleting: {iconType: "progress", iconName: null,       color: red[800]},
  Running:  {iconType: "icon",     iconName: "computer", color: green[500]},
  Deleted:  {iconType: "icon",     iconName: "computer", color: red[800]},
  Unknown:  {iconType: "icon",     iconName: "info",     color: yellow[800]},
  Error:    {iconType: "icon",     iconName: "info",     color: red[800]},
  Stopped:  {iconType: "icon",     iconName: "computer", color: grey[700]}
}


/**
 * Display the status as an icon or progress/loading circle. Color and icon
 * are set in response to cluster status.
 * @props clusterStatus string from clusterStatus enumeration.
 */
class StatusIcon extends React.Component {

  render() {
    console.log(this.props.clusterStatus);
    var status = this.props.clusterStatus;
    var statusMessage = "Status: " + status;
    // Check if status in in the color/icon map. Create an informative status
    // tooltip and set the status (used for icon selection) to 'Unknown',
    if (!(status in statusColorIconMap)) {
      console.log('')
      statusMessage = "Cannot parse '" + status + "'";
      status = "Unknown";
    }
    var color = statusColorIconMap[status].color;
    var iconName = statusColorIconMap[status].iconName;
    var iconType = statusColorIconMap[status].iconType;
    if (iconType === "progress") {
      return (
        <StatusProgress
            statusToolTip={statusMessage}
            color={color}
        />
      );
    }
    return (
      <StatusIconIcon
          statusToolTip={statusMessage}
          color={color}
          iconName={iconName}
      />
    );
  }
}


/**
 * Display an icon and tooltip for the status.
 * @props statusToolTip string shown in icon tooltip.
 * @props color string of hex color code for the icon.
 * @props iconName string material icon name.
 */
class StatusIconIcon extends React.Component {
  render() {
    return (
      <div style={{paddingTop: 10}}>
      <Tooltip id="tooltip-icon-bottom" title={this.props.statusToolTip} >
      <Icon style={{fontSize: 32, color: this.props.color}}>
        {this.props.iconName}
      </Icon>
      </Tooltip>
      </div>
    )
  }
}


/**
 * Display a colored progress spinner and tooltip for the status.
 * @props statusToolTip string shown in icon tooltip.
 * @props color string of hex color code for the icon.
 */
class StatusProgress extends React.Component {
  render() {
    return (
      <div style={{paddingTop: 10}}>
        <Tooltip id="tooltip-bottom" title={this.props.statusToolTip} >
          <CircularProgress style={{color: this.props.color}} />
        </Tooltip>
      </div>
    );
  }
}

export default StatusIcon;
