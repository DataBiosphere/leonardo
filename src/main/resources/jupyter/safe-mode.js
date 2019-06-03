// Adapted from the All of Us Researcher Workbench "Playground Mode"
// https://github.com/all-of-us/workbench/blob/master/api/cluster-resources/playground-extension.js

// TODO: the following is required until IA-979 is implemented
// To deploy on a Leonardo cluster:
// 1. copy to a public GCS location
// 2. issue a PUT request to <leonardo>/api/cluster/<billing project>/<cluster name>
//  with a name:path key value pair set in userJupyterExtensionConfig.nbExtensions

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
    // TODO: query welder for when to enable (IA-979).
    // let's disable this just in case someone accidentally merges the PR
    const enabled = false;
    if (!enabled) {
      return;
    }


    //
    // Disable UI controls
    //


    // Save Notebook button
    //"notbook" is an intentional typo to match the Jupyter UI HTML.
    $('#save-notbook').remove();

    // New Notebook menu tree
    $('#new_notebook').remove();

    // Open... menu item
    $('#open_notebook').remove();

    // Make a Copy... menu item
    $('#copy_notebook').remove();

    // Save as... menu item
    $('#save_notebook_as').remove();

    // Save and Checkpoint menu item
    $('#save_checkpoint').remove();

    // Revert to Checkpoint menu tree
    $('#restore_checkpoint').remove();

    // A little cleanup: remove two dividers in the file menu
    // which are no longer dividing anything
    $("#file_menu .divider")[0].remove();
    $("#file_menu .divider")[0].remove();


    //
    // Disable UI notifications
    //


    $('#save_widget')
        .append(
            '<style>' +
              // e.g. Last Checkpoint: 3 minutes ago
              '.checkpoint_status { display: none; } ' +
              // e.g. (autosaved) or (unsaved changes)
              '.autosave_status { display: none; }' +
              '</style>');

    $('#notification_area')
        .append(
            '<style>' +
              // e.g. Notebook Saved
              '#notification_notebook span { display: none; } ' +
              '</style>');

    // Add our own persistent "Safe Mode" notification next to the other
    // notifications, e.g. kernel status.

    // TODO: convert to tooltip with this text:
    // Safe Mode allows you to explore, change, and run the code,
    // but your edits will not be saved.
    // To save your work, choose Make a Copy from the File menu to
    // make your own version.

    $('#notification_area').prepend(
        '<div id="safe-mode" style="background-color: #FFFFB2;" ' +
          'class="notification_widget btn btn-xs navbar-btn">' +
          '<span>Safe Mode - your edits will not be saved.' +
            '</span></div>');
  };

  return {
    'load_ipython_extension': load
  };
});
