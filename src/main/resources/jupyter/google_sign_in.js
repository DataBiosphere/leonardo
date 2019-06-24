/*
 * This library is designed to run as a Jupyter/JupyterLab extension to refresh the user's
 * Google credentials while using a notebook. This flow is described in more detail here:
 * https://github.com/DataBiosphere/leonardo/wiki/Connecting-to-a-Leo-Notebook#token-refresh
 *
 * Note since this runs inside both Jupyter and JupyterLab, it should not use any
 * libraries/functionality that exists in one but not the other. Examples: node, requireJS.
 */


// TEMPLATED CODE
// Leonardo has logic to find/replace templated values in the format $(...).
// This will be replaced with the real email login hint before uploading to the notebook server.
var loginHint = $(userEmailLoginHint);

// This is refreshed via postMessage from the client app.
var googleClientId = $(defaultClientId);

function receive(event) {
    if (event.data.type == 'bootstrap-auth.response') {
        if (event.source !== window.opener)
            return;
        googleClientId = event.data.body.googleClientId;
    } else if (event.data.type == 'bootstrap-auth.request') {
        if (event.origin !== window.origin)
            return;
        if (!googleClientId)
            return;
        event.source.postMessage({
            "type": "bootstrap-auth.response",
            "body": {
                "googleClientId": googleClientId
            }
        }, event.origin);
    }
}

function startTimer() {
    loadGapi('auth2', function() {
        function doAuth() {
            if (googleClientId) {
                gapi.auth2.authorize({
                    'client_id': googleClientId,
                    'scope': 'openid profile email',
                    'login_hint': loginHint,
                    'prompt': 'none'
                }, function(result) {
                    if (result.error) {
                        console.error("Error occurred authorizing with Google: " + result.error);
                        return;
                    }
                    set_cookie(result.access_token, result.expires_in);
                });
            }
        }

        // refresh token every 3 minutes
        console.log('Starting token refresh timer');
        setInterval(doAuth, 180000);
    });


    function statusCheck() {
        // Leonardo has logic to find/replace templated values in the format $(...).
        // TEMPLATED CODE
        var googleProject = $(googleProject);
        var clusterName = $(clusterName);

        var xhttp = new XMLHttpRequest();
        xhttp.open("GET", "/notebooks/" + googleProject + "/" + clusterName + "/api/status", true);
        xhttp.send();
    }
    setInterval(statusCheck, 60000)
}

function set_cookie(token, expires_in) {
    var expiresDate = new Date();
    expiresDate.setSeconds(expiresDate.getSeconds() + expires_in);
    document.cookie = "LeoToken=" + token + ";secure;expires=" + expiresDate.toUTCString() + ";path=/";
}

function loadGapi(google_lib, continuation) {
    console.log('Loading Google APIs');
    // Get the gapi script from Google.
    const gapiScript = document.createElement('script');
    gapiScript.src = 'https://apis.google.com/js/api.js';
    gapiScript.type = 'text/javascript';
    gapiScript.async = true;

    // Load requested API scripts onto the page.
    gapiScript.onload = function() {
        console.log("Loading Google library '" + google_lib + "'");
        gapi.load(google_lib, continuation);
    }
    gapiScript.onerror = function() {
        console.error('Unable to load Google APIs');
    }
    document.head.appendChild(gapiScript);
}

function init() {
    console.log('Starting google_sign_in extension');
    startTimer();
    window.addEventListener('message', receive);
    if (!googleClientId && window.opener) {
        window.opener.postMessage({ 'type': 'bootstrap-auth.request' }, '*');
    }
}

init();