const dialog = require('base/js/dialog')
const utils = require("base/js/utils")

define(() => {
    // TEMPLATED CODE
    // Leonardo has logic to find/replace templated values in the format $(...).
    const googleProject = $(googleProject)
    const clusterName = $(clusterName)

    let modalOpen = false
        //this needs to be available so the loop can be cancelled where needed
    let syncMaintainer;
    let shouldExit = false

    const syncIssueButtons = (res) => {
        return {
            'Make a Copy': {
                click: () => saveAs(),
                'class': 'btn-primary'
            },
            'Reload the workspace version and discard your changes': {
                click: () => updateLocalCopyWithRemote(res)
            }
        }
    }

    const lockIssueButtons = (res) => {
        return {
            'Run in Playground Mode': {
                click: () => openPlaygroundMode(res),
                'class': 'btn-primary'
            },
            'Make a Copy': {
                click: () => saveAs()
            }
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

    //URLS for leo deployment
    const leoUrl = '' //we are choosing to use a relative path here
    const welderUrl = leoUrl + `/proxy/${googleProject}/${clusterName}/welder`
    const jupyterServerApi = `/notebooks/${googleProject}/${clusterName}` + '/api/contents/'
    const jupyterFsHref = `/notebooks/${googleProject}/${clusterName}/notebooks/`

    //URLS for local testing
    // const jupyterServerApi = '/api/contents/'
    // const jupyterFsHref = '/notebooks/'
    // const leoUrl = 'http://localhost:8080' //for testing against local server
    // const welderUrl = leoUrl

    const localizeUrl = welderUrl + '/objects'
    const checkMetaUrl = welderUrl + '/objects/metadata'
    const lockUrl = welderUrl + '/objects/lock'
    const lastLockedTimer = 60000

    const headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Headers': '*'
    }

    const basePayload = {
        mode: "no-cors",
        headers: headers
    }

    function init() {
        console.info('edit mode plugin initialized')
        checkMeta()
        initSyncMaintainer()
    }

    function initSyncMaintainer() {
        syncMaintainer = setInterval(() => {
            checkMeta()
        }, lastLockedTimer)

        window.onbeforeunload(function() {
            clearInterval(syncMaintainer)
        })
    }

    function checkMeta() {
        const localPath = {
            localPath: Jupyter.notebook.notebook_path
        }

        console.info('calling /objects/metadata/ with payload: ', JSON.stringify(localPath))

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
                handleMetaFailure(err)
            })
    }

    function renderFileNotTrackedBanner() {
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
                console.warn('detected 412 from /objects/metadata. stopping loop')
                renderFileNotTrackedBanner()
                shouldExit = true
                clearInterval(syncMaintainer)
            }

            throw Error(res.statusText)
        }
    }

    function handleMetaSuccess(res) {
        handleCheckMetaResp(res) //displays modal if theres an issue in the payload
        toggleMetaFailureBanner(false) //sets banner for meta status
        maintainLockState(res) //gets lock if in edit mode
        maintainModeBanner(res) //sets edit/safe mode banner
    }

    function handleMetaFailure(err) {
        console.error(err)

        if (!shouldExit) {
            removeElementById(modeBannerId)
            toggleMetaFailureBanner(true)
        }
    }

    //this function assumes any status not included in these lists represents a notebook out of sync to defend against future fields being added being auto-categorized as failures
    function handleCheckMetaResp(res) {

        const healthySyncStatuses = ["LIVE"]
        const outOfSyncStatuses = ["DESYNCHRONIZED", "LOCAL_CHANGED", "REMOTE_CHANGED"] //not used but here for reference
        const notFoundStatus = ["REMOTE_NOT_FOUND"]

        if (healthySyncStatuses.includes(res.syncStatus)) {
            console.info('healthy sync status detected: ', res.syncStatus)
        } else if (notFoundStatus.includes(res.syncStatus)) {
            promptUserWithModal(syncIssueTitle, noRemoteFileButtons, syncIssueNotFoundBody)
        } else {
            promptUserWithModal(syncIssueTitle, syncIssueButtons(res), syncIssueBody)
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

    function getLock() {
        const payload = {
            ...basePayload,
            method: 'POST',
            body: JSON.stringify({ localPath: Jupyter.notebook.notebook_path })
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

    function toggleMetaFailureBanner(shouldShow) {

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

    const lockConflictBody = `<p>This file is currently being edited by another user.</p>` +
        `<br/><p>You can make a copy, or run it in Playground Mode to explore and execute its contents without saving any changes.`;

    function handleLockStatus(res) {
        if (!res.ok) {
            const status = res.status
            const errorText = res.statusText

            if (status == 409) {
                res.json().then(res => {
                    promptUserWithModal(lockConflictTitle, lockIssueButtons(res), lockConflictBody)
                })
            }
            //for the lock endpoint, we consider all non 'ok' statuses an error
            throw new Error(errorText)
        }
    }


    function promptUserWithModal(title, buttons, htmlBody) {
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
            .find(".close").remove() //TODO: test going back
    }

    async function openPlaygroundMode(meta) {
        const originalNotebookName = Jupyter.notebook.notebook_name

        const safeModeDir = meta.storageLink.localSafeModeBaseDirectory

        if (Jupyter.notebook.notebook_path.includes(safeModeDir)) {
            console.warn('Attempted to navigate to enter safe mode while already in safe mode. Exitting.')
            return; //we're here already
        }

        //create a new file with the contents
        const postPayload = {
            ...basePayload,
            method: 'POST',
            body: JSON.stringify({
                copy_from: Jupyter.notebook.notebook_path
            })
        }

        const patchPayload = {
            headers: headers,
            method: 'PATCH',
            body: JSON.stringify({
                path: safeModeDir + '/' + originalNotebookName
            })
        }

        fetch(jupyterServerApi + safeModeDir, postPayload)
            .then(res => res.json())
            .then(res => {
                //then we rename the file, as POST does not allow us to specify the file name
                fetch(jupyterServerApi + res.path, patchPayload)
                    .then(res => {
                        //navigate to new file
                        window.location.href = jupyterFsHref + safeModeDir + '/' + originalNotebookName
                    })

            })
    }

    function saveAs() {
        const originalPathSplit = utils.url_path_split(Jupyter.notebook.notebook_path) //guarantees a path in [0] and file name in [1]. [0] is "" if just a file is passed
        const newNotebookPath = originalPathSplit[0]

        //create a new file with the contents
        const payload = {
            ...basePayload,
            method: 'POST',
            body: JSON.stringify({
                copy_from: Jupyter.notebook.notebook_path
            })
        }

        fetch(jupyterServerApi + newNotebookPath, payload)
            .then(res => res.json())
            .then(res => {
                //navigate to new file. we rely on the jupyter post api to supply the name of the file we have created as it ensures it does not exist
                //POST also does not allow for the specification of a file name 
                window.location.href = jupyterFsHref + res.path
            })
    }

    function removeElementById(id) {
        if (!$("#" + id).length == 0) {
            $("#" + id).remove()
        }
    }

    //shows the user whether they are in playground mode or edit mode
    function renderModeBanner(isEditMode) {
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
            action: "localize",
            entries: [{
                sourceUri: meta.storageLink.cloudStorageDirectory + '/' + Jupyter.notebook.notebook_name,
                localDestinationPath: Jupyter.notebook.notebook_path
            }]
        }

        const payload = {
            ...basePayload,
            method: 'POST',
            body: JSON.stringify(entries)
        }

        await fetch(localizeUrl, payload)

        location.reload(true)
    }

    init()
})