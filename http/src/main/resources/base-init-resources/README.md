# Jupyter nbextension development guide

## Running locally
To run plugins off local js files:
1. [Install jupyter](https://jupyter.org/install)
2. Run this command with the arguments to ensure the extension updates when you make changes to the .js files: `jupyter nbextension install /[absolute path to leo repo]/leonardo/src/main/resources/jupyter/ --symlink`
3. To test edit-mode or safe-mode extensions, you must update the file to use local urls. For edit-mode, you can find a section of 4 variables near the top labelled `URLS for local testing` and a section labelled `URLS for leo deployment` above it. You can comment out the `URLS for leo deployment` and uncomment  `URLS for local testing` (TODO: find a better way to do this)
4. Run this for each extension in the jupyter/ dir you want enabled: ```jupyter nbextension enable jupyter/[File name WITHOUT EXTENSION]``` I.E., `jupyter nbextension enable jupyter/edit-mode`
5. Run `jupyter notebook`. It should open the jupyter server in the browser window. You can verify the appropriate extension loaded via openning the developer console abd going to the `Sources` tab. On the file explorer on the left, you should find a folder called `nbextensions` containing the loaded extensions, possibly in `nbextensions -> jupyter`. Here you can place breakpoints to test functionality.

## Misc Info

Look at the jupyter_notebook_config and ensure your local config emulates what the settings are found in this file (of interest are port number and cors/auth settings)

At the time of writing, there are 3 nbextensions, edit-mode.js, safe-mode.js, and google_sign_in.js

extension_entry.js controls which plugins are loaded into the jupyter server image

POST storageLinks/:
`curl -vX POST --header 'Content-Type: application/json' --header 'Accept: application/json' [welderUrl]/storageLinks -d '{"localBaseDirectory": "[local dir relative to dir in welder conf, ex 'edit']", "localSafeModeBaseDirectory": "[local dir relative to dir in welder conf, ex 'safe']", "cloudStorageDirectory": "gs://jc-sample-bucket", "pattern": "*" }'`

POST localize/:
`curl -vX POST --header 'Content-Type: application/json' --header 'Accept: application/json' localhost:8081/objects -d '{"action" : "localize",  "entries": [{ "sourceUri": "gs://jc-sample-bucket/Untitled.ipynb", "localDestinationPath": "edit/Untitled.ipynb" }] }'`
