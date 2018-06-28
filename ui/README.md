This project was bootstrapped with [Create React App](https://github.com/facebookincubator/create-react-app).

The full guide, with information on common tasks such as build, testing,
environment configuration and more available [here](https://github.com/facebookincubator/create-react-app/blob/master/packages/react-scripts/template/README.md).

### Common development tasks

Start development server:

    ```sh
    npm start
    ```

Build a binary release (outputs to ./build):
    ```sh
    npm run build
    ```

Add a dependency:
    ```sh
    npm install --save react-router
    ```


### Environment configuration

Edit the contents of public/config.json. This file is loaded
synchronously during application startup into the global variable
`GlobalReactConfig`.

The UI can be dockerized and each value in the config.json may
be passed as an environmental variable thanks to a shell script
that manages these substitutions. As a result, if any new
config variables are added, the file "write_config.sh" should
be updated to be made aware of the new values.
