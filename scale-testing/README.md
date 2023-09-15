To use:

Export environment variables:
```bash
export AZURE_TOKEN=abcd-123
#export BEE_NAME=my-bee
export ENV=prod
export SOURCE_BILLING_PROJECT_NAME=BroadE-0923-definition
export DEST_BILLING_PROJECT_NAME=BroadE-0923
export WORKSPACE_NAME=Terra-on-Azure_Quickstart
```
Edit variables in `cloneTest.py`:
- `number_of_clones` number of clones to make
- `parallel` if false, will wait for each api clone call to complete before starting the next. 
if true, will start all api calls at once on different threads

You may also want to update the amount of time for sleeping:
on line 42, this prevents errors from the API calls happening too quickly together.
On line 53, this gives the cloned WDSs time to start up before checking them for success.

```bash
pip install -r requirements.txt
python cloneTest.py
```