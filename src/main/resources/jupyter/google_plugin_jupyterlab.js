module.exports = [{
    id: 'google_plugin_jupyterlab',
    autoStart: true,
    activate: function(app) {
        require('/home/jupyter-user/.jupyter/custom/google_sign_in');
    }
}];