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

// This is refreshed via postMessage from the client app.
var googleClientId;

function receive(event) {
    if (event.data.type == 'bootstrap-auth.response') {
        if (event.source !== window.opener)
            return;
        googleClientId = event.data.body.googleClientId;
    }

    else if (event.data.type == 'bootstrap-auth.request') {
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
    require(['gapi'], function (gapi) {
        gapi.load('auth2', function () {
            function doAuth() {
                if (googleClientId) {
                    gapi.auth2.authorize({
                        'client_id': googleClientId,
                        'scope': 'openid profile email',
                        'login_hint': loginHint,
                        'prompt': 'none'
                    }, function (result) {
                        if (result.error) {
                            return;
                        }
                        set_cookie(result.access_token, result.expires_in);
                    });
                }
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

function init() {
    startTimer();
    window.addEventListener('message', receive);
    if (googleClientId == null) {
        window.opener.postMessage({'type': 'bootstrap-auth.request'}, '*');
    }
}

init();
