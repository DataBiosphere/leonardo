import React from 'react';

import AppBar from 'material-ui/AppBar';
import Button from 'material-ui/Button';
import Icon from 'material-ui/Icon';
import Toolbar from 'material-ui/Toolbar';
import Typography from 'material-ui/Typography';
import { withStyles } from 'material-ui/styles';



/**
 * Display an app bar with tile, icon, and refresh button.
 * @props cardsRefreshHandler function callback that refreshes the state
 *        of the ListStateContainer by calling the Leo API.
 */
class UnstyledLeonardoAppBar extends React.Component {
  render() {
    var classes = this.props.classes;
    return (
      <div className={classes.root}>
        <AppBar position="static">
        <Toolbar>
          <span className="leoAppBarIcon">
            <Icon
              className={classes.icon}
              color="inherit"
              aria-label="Menu"
            >
              assignment
            </Icon>
          </span>
          <Typography
            variant="title"
            color="inherit"
            className={classes.flex}
          >
            Leonardo Notebooks
          </Typography>
          <Button
            color="inherit"
            onClick={this.props.cardsRefreshHandler}
          >
            <Icon
              color="inherit"
              aria-label="refresh"
            >
                refresh
            </Icon>
            Refresh
          </Button>
        </Toolbar>
        </AppBar>
      </div>
    );
  }
}

export default withStyles(
  {
    root: {
      flexGrow: 1,
    },
    flex: {
      flex: 1,
    },
    icon: {
      marginLeft: 0,
      marginRight: 20,
    }
  })(UnstyledLeonardoAppBar)