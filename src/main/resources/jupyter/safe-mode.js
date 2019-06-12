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

// const namespace = require('base/js/namespace')

define(() => {
    function load() {
        checkMetaLoop()
    }

    function checkMetaLoop() {
        toggleUIControls(isSafeMode())

        const interval = setInterval(() => {
            toggleUIControls(isSafeMode())
        }, 6000)

        window.onbeforeunload(() => {
            clearInterval(interval)
        })
    }

    function isSafeMode() {
        return false //TODO figure out what the welder link will be relative to the jupyter image
    }

    function toggleUIControls(shouldHide) {
        //these are the jquery selectors for the elements we will toggle
        //"notbook" is an intentional typo to match the Jupyter UI HTML.
        const selectorsToHide = ['#save-notbook', '#new_notebook', '#open_notebook', '#copy_notebook', '#save_notebook_as', '#save_checkpoint', '#restore_checkpoint', '.checkpoint_status', '.autosave_status', '#notification_notebook', '#file_menu > li.divider:eq(0)', '#file_menu > li.divider:eq(2)']
        selectorsToHide.forEach((selector) => {
            if (shouldHide) {
                $(selector).hide()
            } else {
                $(selector).show()
            }
        })
    }

    return {
        'load_ipython_extension': load
    };
});