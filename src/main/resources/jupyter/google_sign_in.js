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

/// TEMPLATED CODE
var clientId = $(googleClientId);
///

require(['gapi'], function(gapi) {
    gapi.load('auth2', function() {
        gapi.auth2.init({
            client_id: clientId,
            scope: 'email profile openid',
        }).then(function() {
            auth2 = gapi.auth2.getAuthInstance();
            auth2.currentUser.listen(function(user) {
                authResponse = user.getAuthResponse();
                set_cookie(authResponse.access_token, authResponse.expires_in);
            });
            if (auth2.isSignedIn.get() == false) {
                auth2.signIn();
            }
        });
    });
});

function set_cookie(token, expires_in) {
    var now = new Date();
    now.setSeconds(now.getSeconds() + expires_in);
    document.cookie = "FCtoken="+token+";expires="+now.toUTCString();
}