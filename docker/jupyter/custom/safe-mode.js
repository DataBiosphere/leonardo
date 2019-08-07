// Adapted from the All of Us Researcher Workbench "Playground Mode"
// https://github.com/all-of-us/workbench/blob/master/api/cluster-resources/playground-extension.js

// In "Safe Mode", changes are not saved back to GCS. This extension makes
// minor UI tweaks to differentiate this mode from normal Jupyter usage, and
// also removes/hides controls relating to persistence. Technically
// this does not stop autosave from continuing to happen in the background, but
// the intended use of this plugin is in a separate space from normal operation
// which does not support localization features.

// const namespace = require('base/js/namespace')

define(() => {
    // define default values for config parameters
    var params = {
        googleProject: '',
        clusterName: ''
    };

    // update params with any specified in the server's config file
    function updateParams() {
        var config = Jupyter.notebook.config;
        for (var key in params) {
            if (config.data.hasOwnProperty(key))
                params[key] = config.data[key];
        }

        // generate URLs based on params
        let welderUrl = `/proxy/${params.googleProject}/${params.clusterName}/welder`
        params.checkMetaUrl = welderUrl + '/objects/metadata'
    }

    const headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Headers': '*'
    }

    const basePayload = {
        mode: "no-cors",
        headers: headers
    }

    function load() {
        console.info('safe mode plugin initialized')

        if (!Jupyter.notebook) {
            return; //exit, they are in list view
        }

        updateParams()
        checkMetaLoop()
    }

    function checkMetaLoop() {
        triggerUIToggle()

        const interval = setInterval(() => {
            triggerUIToggle()
        }, 60000)

        window.onbeforeunload(() => {
            clearInterval(interval)
        })
    }

    async function triggerUIToggle() {
        checkMeta()
            .then(res => {
                if (res.syncMode == "EDIT") {
                    toggleUIControls(false)
                } else {
                    //there is an icon in the jupyter UI that has a tool tip that says 'edit mode'.
                    //this shows up whenever a user types, so we change the tool-tip to avoid confusion
                    $("#modal_indicator").tooltip({ "content": "Adding code" });
                    toggleUIControls(true)
                }
            })
            .catch(err => {
                console.error(err)
                toggleUIControls(false) //we always assume safe mode if the check meta call fails
            })
    }

    function checkMeta() {
        const payload = {
            ...basePayload,
            body: JSON.stringify({ localPath: Jupyter.notebook.notebook_path }),
            method: 'POST'
        }

        return fetch(params.checkMetaUrl, payload)
            .then(res => {
                if (!res.ok) {
                    throw Error("check metadata call failed due to status code")
                }
                return res.json()
            })
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

    load()
});