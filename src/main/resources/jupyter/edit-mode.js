const dialog = require('base/js/dialog')
const utils = require("base/js/utils")

// TEMPLATED CODE
// Leonardo has logic to find/replace templated values in the format $(...).

// let googleProject = $(googleProject);
// let clusterName = $(clusterName);

define(() => {
    console.log('here in define')
    let modalOpen = false
    let meta = {}
        //this needs to be available to the loop can be cancelled where needed
    let syncMaintainer = 1
    let shouldExit = false

    const syncIssueButtons = {
        'Make a Copy': {
            click: () => saveAs(),
            'class': 'btn-primary'
        },
        'Reload the workspace version and discard your changes': {
            click: () => updateLocalCopyWithRemote()
        }
    }

    const lockIssueButtons = {
        'Run in Playground Mode': {
            click: () => openPlaygroundMode(),
            'class': 'btn-primary'
        },
        'Make a Copy': {
            click: () => saveAs()
        }
    }

    const noRemoteFileButtons = {
        'Continue working': {
            click: () => {},
            'class': 'btn-primary'
        }
    }

    const modeBannerId = "notification_mode"
    const lockConflictTitle = "File is in use"
    const syncIssueTitle = "File versions out of sync"
    const syncIssueBody = "Your version of this file does not match the version in the workspace. What would you like to do?"
    const syncIssueNotFoundBody = "This file was either deleted or never was stored with us."

    //TODO URL resolution
    // const leoUrl = ''
    const leoUrl = 'http://localhost:8080'
        // const welderUrl = leoUrl + `/proxy/${googleProject}/${clusterName}/welder`
    const welderUrl = leoUrl
    const localizeUrl = welderUrl + '/localize'
        // const checkMetaUrl = welderUrl + '/checkMeta'
    const checkMetaUrl = welderUrl + '/objects/metadata'
    const lockUrl = welderUrl + '/lock'
    const lastLockedTimer = 60000 // in ms, should be 60000 in final PR

    const headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Headers': '*'
    }

    const basePayload = {
        mode: "no-cors",
        headers: headers
    }

    //TODO resolve these links
    const jupyterBaseUrl = `/notebooks/${googleProject}/${clusterName}`
    const jupyterContentsAPIUrl = jupyterBaseUrl + "/api/contents/"

    function init() {
        console.log('here in edit mode init')
            // checkMeta()
            // initSyncMaintainer()
    }

    async function initSyncMaintainer() {
        syncMaintainer = setInterval(() => {
            checkMeta()
        }, lastLockedTimer)

        window.onbeforeunload(function() {
            clearInterval(syncMaintainer)
        })
    }

    async function checkMeta() {
        const localPath = {
            localPath: Jupyter.notebook.notebook_path
        }

        console.log('sending payload: ', JSON.stringify(localPath))

        const payload = {
            ...basePayload,
            body: JSON.stringify(localPath),
            method: 'POST'
        }

        return fetch(checkMetaUrl, payload)
            .then(res => {
                processInitialCheckMeta(res)
                return res.json()
            })
            .then(res => {
                handleMetaSuccess(res)
                return res
            })
            .catch(err => {
                console.log('here')
                handleMetaFailure(err)
            })
    }

    function renderFileNotTrackedBanner() {
        console.log('here2')
        removeElementById(modeBannerId)

        let toolTipText = "<p>Your changes are not being saved to the workspace.</p>"

        $('#notification_area').append(
            $('<div>').attr({
                "id": "notification_not_saving",
                "class": "btn-warning btn btn-xs navbar-btn",
                "data-toggle": "tooltip",
                "data-html": "true",
                "title": toolTipText
            })
            .tooltip({
                content: function() { return $(this).prop('title'); },
                "placement": "bottom"
            })
            .append(
                $('<span>').html("Remote Save Disabled")
                .append(' <i class="fa fa-question-circle" aria-hidden="true"></i>')
            )
        );
    }

    function processInitialCheckMeta(res) {
        if (!res.ok) {
            if (res.status == 412) {
                console.log('detected 412')
                renderFileNotTrackedBanner()
                shouldExit = true
                clearInterval(syncMaintainer)
            }

            throw Error(res.statusText)
        }
    }

    function handleMetaSuccess(res) {
        console.log('res in check meta after .json()', res)
        handleCheckMetaResp(res) //sets meta state
        toggleMetaFailureBanner(false) //sets banner for meta status
        maintainLockState(res) //gets lock if in edit mode
        maintainModeBanner(res)
    }

    function handleMetaFailure(err) {
        console.log('in checkMeta catch')
        console.error(err)

        if (!shouldExit) {
            removeElementById(modeBannerId)
            toggleMetaFailureBanner(true)
        }
    }

    //this function assumes any status not included in these lists represents an in out of sync notebook to defend against future fields being added being auto-categorized as failures
    function handleCheckMetaResp(res) {
        meta = res //set meta state

        const healthySyncStatuses = ["LIVE"]
        const outOfSyncStatuses = ["DESYNCHRONIZED", "LOCAL_CHANGED", "REMOTE_CHANGED"]
        const notFoundStatus = ["REMOTE_NOT_FOUND"]

        if (healthySyncStatuses.includes(res.syncStatus)) {
            console.info('healthy sync status detected')
        } else if (notFoundStatus.includes(res.syncStatus)) {
            promptUserWithModal(syncIssueTitle, noRemoteFileButtons, syncIssueNotFoundBody)
        } else {
            promptUserWithModal(syncIssueTitle, syncIssueButtons, syncIssueBody)
        }
    }

    function maintainLockState(res) {
        const isEditMode = res.syncMode == "EDIT"
        if (isEditMode) {
            getLock()
        }
    }

    function maintainModeBanner(res) {
        const isEditMode = res.syncMode == "EDIT"
        renderModeBanner(isEditMode)
    }

    async function getLock() {
        const localPath = { localPath: Jupyter.notebook.notebook_path }

        const payload = {
            ...basePayload,
            method: 'POST',
            body: JSON.stringify(localPath)
        }

        fetch(lockUrl, payload)
            .then(res => {
                handleLockStatus(res)
                return res.json()
            })
            .catch(err => {
                console.error(err)
            })
    }

    async function toggleMetaFailureBanner(shouldShow) {

        const bannerId = "notification_metaFailure"
        const bannerText = "Failed to check notebook status, changes may not be saved to workspace. Retrying..."

        removeElementById(bannerId)

        if (shouldShow) {
            const bannerStyling = "btn btn-xs navbar-btn btn-danger"

            $('#notification_area').append(
                $('<div>').attr({
                    "id": bannerId,
                    "class": bannerStyling
                })
                .append($('<span>').html('<i class="fa fa-exclamation-triangle"></i> ' + bannerText))
            );
        }

    }

    async function handleLockStatus(res) {
        if (!res.ok) {
            const status = res.status
            const errorText = res.statusText

            if (status == 409) {
                res.json().then(body => {
                    const message = getLockConflictBody(body.lockedBy)
                    promptUserWithModal(lockConflictTitle, lockIssueButtons, message)
                })
            }
            //for the lock endpoint, we consider all non 'ok' statuses an error
            throw new Error(errorText)
        }
    }

    const getLockConflictBody = () => {
        return `<p>This file is currently being edited by another user.</p>` +
            `<br/><p>You can make a copy, or run it in Playground Mode to explore and execute its contents without saving any changes.`;
    }

    async function promptUserWithModal(title, buttons, htmlBody) {
        if (modalOpen) return

        modalOpen = true

        dialog.modal({
                body: $('<p>').html(htmlBody),
                title: title,
                buttons: buttons,
                notebook: Jupyter.notebook,
                keyboard_manager: Jupyter.notebook.keyboard_manager
            })
            .on('hidden.bs.modal', () => modalOpen = false)
            .attr('id', 'leoUserModal')
            .find(".close").click(() => window.history.back()) //TODO: test in docker image
    }

    async function openPlaygroundMode() {
        const url = jupyterContentsAPIUrl + Jupyter.notebook.notebook_path

        //TODO fix path
        const newPath = meta.storageLink.localSafeModeBaseDirectory.replace('notebooks/', '') + '/' + Jupyter.notebook.notebook_name

        console.log('switching to playground path: ', newPath)

        //here we are calling the jupyter server API to PATCH the file they are currently working 
        //this call results in the notebook currently being worked on saved into the custom playground mode directory
        //
        const payload = {
            ...basePayload,
            method: 'PATCH',
            body: JSON.stringify({ path: newPath })
        }

        console.log('url: ', url, newPath)

        fetch(url, payload).then(res => {
            window.location.href = jupyterBaseUrl + "notebooks/" + newPath
        })
    }

    async function saveAs() {
        const url = jupyterContentsAPIUrl + Jupyter.notebook.notebook_path

        const originalPathSplit = utils.url_path_split(Jupyter.notebook.notebook_path)
        const originalFileSplit = utils.splitext(originalPathSplit[1])

        const newNotebookName = originalFileSplit[0] + "_COPY" + originalFileSplit[1]
        const newNotebookPath = originalPathSplit[0] + '/' + newNotebookName

        const payload = {
            ...basePayload,
            method: 'PATCH',
            body: JSON.stringify({ path: newNotebookPath }) // body data type must match "Content-Type" header
        }

        const currHrefSplit = utils.url_path_split(window.location.href)
        const newHref = currHrefSplit[0] + "/" + newNotebookName

        console.log('url: ', url, newNotebookPath)

        fetch(url, payload).then(res => {
            window.location.href = newHref
        })
    }

    async function removeElementById(id) {
        if (!$("#" + id).length == 0) {
            $("#" + id).remove()
        }
    }

    //shows the user whether they are in playground mode or edit mode
    async function renderModeBanner(isEditMode) {
        removeElementById(modeBannerId) //we always remove the banner because we re-render each loop

        let bannerText;
        let toolTipText;
        let bannerStyling;

        const baseStyling = "btn btn-xs navbar-btn"

        if (isEditMode) {
            bannerText = "Edit Mode"
            toolTipText = "Your changes are being saved to the workspace."
            bannerStyling = "notification_widget " + baseStyling;
        } else {
            bannerText = "PLAYGROUND MODE (Edits not saved)"
            toolTipText = "<p>Playground mode allows you to explore, change, and run the code, but your edits will not be saved. </p><br/><p>To save your work, choose Make a Copy from the File menu to make your own version.</p>"
            bannerStyling = "btn-warning " + baseStyling
        }

        $('#notification_area').append(
            $('<div>').attr({
                "id": modeBannerId,
                "class": bannerStyling,
                "data-toggle": "tooltip",
                "data-html": "true",
                "title": toolTipText
            })
            .tooltip({
                content: function() { return $(this).prop('title'); },
                "placement": "bottom"
            })
            .append(
                $('<span>').html(bannerText)
                .append(' <i class="fa fa-question-circle" aria-hidden="true"></i>')
            )
        );
    }

    async function updateLocalCopyWithRemote(meta) {
        const entries = {
            entries: {
                sourceUri: meta.remoteUri + '/' + Jupyter.notebook.notebook_name,
                localDestinationPath: Jupyter.notebook.notebook_path
            }
        }

        const payload = {
            ...basePayload,
            method: 'POST',
            body: JSON.stringify(entries)
        }

        fetch(localizeUrl, payload)
            .then(res => {
                location.reload(true) //is this needed?
            }).catch(err => {
                location.reload(true)
            })
    }

    return {
        load_ipython_extension: init
    }
})