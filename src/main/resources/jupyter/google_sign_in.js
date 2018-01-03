require.config({
    "shim": {
        "gapi": {
            "exports": "gapi"
        }
    },
    "paths": {
        "gapi": "https://apis.google.com/js/platform"
    }
})

// TEMPLATED CODE
// Leonardo has logic to find/replace templated values in the format $(...).
// This will be replaced with the real email login hint before uploading to the notebook server.
var loginHint = $(userEmailLoginHint);

function receive(event) {
    if (event.source !== window.opener)
        return;
    if (event.data.type !== 'bootstrap-auth.response')
        return;
    startTimer(event.data.body.googleClientId);
}

function startTimer(googleClientId) {
    require(['gapi'], function(gapi) {
        gapi.load('auth2', function() {
            function doAuth() {
                gapi.auth2.authorize({
                    'client_id': googleClientId,
                    'scope': 'openid profile email',
                    'login_hint': loginHint,
                    'prompt': 'none'
                }, function(result) {
                    if (result.error) {
                        return;
                    }
                    set_cookie(result.access_token, result.expires_in);
                });
            }
            // refresh token every 2 minutes
            setInterval(doAuth, 120000);
        });
    });
}

function set_cookie(token, expires_in) {
    var expiresDate = new Date();
    expiresDate.setSeconds(expiresDate.getSeconds() + expires_in);
    document.cookie = "FCtoken="+token+";secure;expires="+expiresDate.toUTCString()+";path=/";
}

window.addEventListener('message', receive);
window.opener.postMessage('bootstrap-auth.request', '*');