/*
 * This library is designed to run as a Jupyter/JupyterLab extension to refresh the user's
 * Google credentials while using a notebook. This flow is described in more detail here:
 * https://github.com/DataBiosphere/leonardo/wiki/Connecting-to-a-Leo-Notebook#token-refresh
 *
 * Note since this runs inside both Jupyter and JupyterLab, it should not use any
 * libraries/functionality that exists in one but not the other. Examples: node, requireJS.
 */


// define default values for config parameters
var params = {
    loginHint: '',
    googleClientId: ''
};

// update params with any specified in the server's config file
function updateParams() {
    var config = Jupyter.notebook.config;
    for (let key in params) {
        if (config.data.hasOwnProperty(key))
            params[key] = config.data[key];
    }
}

function receive(event) {
    if (event.data.type == 'bootstrap-auth.response') {
        if (event.source !== window.opener)
            return;
        params.googleClientId = event.data.body.googleClientId;
    } else if (event.data.type == 'bootstrap-auth.request') {
        if (event.origin !== window.origin)
            return;
        if (!params.googleClientId)
            return;
        event.source.postMessage({
            "type": "bootstrap-auth.response",
            "body": {
                "googleClientId": params.googleClientId
            }
        }, event.origin);
    }
}

function startTimer() {
    loadGapi('auth2', function() {
        function doAuth() {
            if (params.googleClientId) {
                gapi.auth2.authorize({
                    'client_id': params.googleClientId,
                    'scope': 'openid profile email',
                    'login_hint': params.loginHint,
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
    updateParams();
    startTimer();
    window.addEventListener('message', receive);
    if (!params.googleClientId && window.opener) {
        window.opener.postMessage({ 'type': 'bootstrap-auth.request' }, '*');
    }
}

init();