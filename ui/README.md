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

Edit the contents of the `.env` file to inject environmental
variables into the project. Every variable that begins with
`REACT_APP_*` will be available via the global
`process.env.REACT_APP_<NAME>`. Variables set in `.env` become
hard coded during build.
