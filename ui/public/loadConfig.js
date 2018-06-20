// Load a json config.


// Run loader as an anonymous function to protect the global namespace.
// This function accesses a file "config.json" expected to be at the
// root of the server and loads it into the global `GlobalReactConfig`.
(function () {
    var xobj = new XMLHttpRequest();
    xobj.overrideMimeType("application/json");
    // Run this in synchronous mode since the configuration should be loaded
    // before the React app initializes.
    xobj.open('GET', 'config.json', false);
    xobj.onreadystatechange = function () {
        if (xobj.readyState == 4 && xobj.status == "200") {
            window.GlobalReactConfig = JSON.parse(xobj.responseText);
        } else {
            console.log("fatal error: config could not be loaded")
        }
    };
    xobj.send(null);
})()