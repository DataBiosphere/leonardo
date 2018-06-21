define([
    'base/js/events'
], function(events) {
    require(['custom/google_sign_in'])
});

// Put the notebook in single tab mode
// See https://jupyter-notebook.readthedocs.io/en/latest/public_server.html?highlight=server#embedding-the-notebook-in-another-website
define([
    'base/js/namespace'
], function(Jupyter) {
    Jupyter._target = '_self';
});