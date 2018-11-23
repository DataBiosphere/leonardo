## Python testing

TODO: Integrate this into automated testing processes.

- Initialize and activate a virtualenv
- Install requirements:

  ```
  jupyter-server$ pip3 install -r test-requirements.txt
  ```
- Run the tests:

  ```
  jupyter-server$ PYTHONPATH=$PYTHONPATH:. python3 -m nose
  ```

