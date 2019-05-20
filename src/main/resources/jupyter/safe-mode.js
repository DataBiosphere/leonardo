// Adapted from the All of Us Researcher Workbench "Playground Mode"
// https://github.com/all-of-us/workbench/blob/master/api/cluster-resources/playground-extension.js

// In "Safe Mode", changes are not saved back to GCS. This extension makes
// minor UI tweaks to differentiate this mode from normal Jupyter usage, and
// also removes/hides controls relating to persistence. Technically
// this does not stop autosave from continuing to happen in the background, but
// the intended use of this plugin is in a separate space from normal operation
// which does not support localization features.

define([
    'base/js/namespace'
], (Jupyter) => {
  const load = () => {
    // TODO always-on, for now
    const enabled = true;
    if (!enabled) {
      return;
    }

    // Disable UI controls/notifications relating to saving.

    // "notbook" is an intentional typo to match Jupyter UI HTML.
    $('#save-notbook').remove();
    $('#save_notebook_as').remove();
    $('#save_checkpoint').remove();

    // TODO: not yet working as intended
    $('#menubar-container')
        .append(
            '<style>' +
              '.autosave_status { display: none; }' +
              '#safe-mode { background-color: #FFFFB2; }' +
              '</style>');

    // Add our own persistent "Safe Mode" notification next to the other
    // notifications, e.g. kernel status.

    // TODO: convert to tooltip

    $('#notification_area').prepend(
        '<div id="safe-mode" class="notification_widget btn btn-xs navbar-btn">' +
          '<span>Safe Mode allows you to explore, change, and run the code, ' +
          'but your edits will not be saved.</span>' +
          '<span>To save your work, choose Make a Copy from the File menu to ' +
          'make your own version.</span>' +
          '</div>');
  };

  return {
    'load_ipython_extension': load
  };
});
