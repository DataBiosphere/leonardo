module.exports = [{
    id: 'example_lab_extension',
    autoStart: true,
    activate: function(app) {
      console.log('JupyterLab extension example_lab_extension is activated!');
      console.log(app.commands);
    }
}];