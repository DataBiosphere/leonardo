import subprocess
import os
import pipes
import tornado
from tornado import gen
from tornado.web import HTTPError
from notebook.base.handlers import IPythonHandler
from notebook.utils import url_path_join

class LocalizeHandler(IPythonHandler):
  def sanitize(self, pathstr):
    """Expands to absolute paths, makes intermediate dirs, and quotes to remove any shell naughtiness.
    This doesn't need to be a coroutine as it's an inline function, not something Future-y."""
    #expanduser behaves fine with gs:// urls, thankfully
    expanded = os.path.expanduser(pathstr)
    if not pathstr.startswith("gs://"):
      try:
        os.makedirs(os.path.dirname(expanded))
      except OSError: #thrown if dirs already exist
        pass
    return pipes.quote(expanded)

  @gen.coroutine
  def localize(self, pathdict):
    """Treats the given dict as a string/string map and sends it to gsutil."""
    all_success = True
    #This gets dropped inside the user's notebook working directory
    with open("localization.log", 'a', buffering=1) as locout:
      for key in pathdict:
        #NOTE: keys are destinations, values are sources
        source = self.sanitize(pathdict[key])
        dest = self.sanitize(key)

        if source.startswith('data:'):
          # if the source is a data URI,
          try:
            uri = DataURI(source)
          except ValueError:
            all_success = False

          # TODO
        else if source.startswith('gs:') or dest.startswith('gs:'):
          cmd = ['gsutil', '-m', '-q', 'cp', '-R', '-c', '-e', source, dest]
          locout.write(' '.join(cmd) + '\n')
          code = subprocess.call(cmd, stderr=locout)
          if code is not 0:
            all_success = False
        else raise
    return all_success

  def post(self):
    try:
      pathdict = tornado.escape.json_decode(self.request.body)
    except ValueError:
      raise HTTPError(400, "Body must be JSON object of type string/string")

    if type(pathdict) is not dict:
      raise HTTPError(400, "Body must be JSON object of type string/string")

    if not all(map(lambda v: type(v) is unicode, pathdict.values())):
      raise HTTPError(400, "Body must be JSON object of type string/string")

    async = self.get_query_argument('async', False)

    if async:
      #complete the request HERE, without waiting for the localize to run
      self.set_status(200)
      self.finish()

      #fire and forget the actual work -- it'll log to a file in the user's homedir
      tornado.ioloop.IOLoop.current().spawn_callback(self.localize, pathdict)

    else:
      #run localize synchronous to the HTTP request
      #run_sync() doesn't take arguments, so we must wrap the call in a lambda.
      success = tornado.ioloop.IOLoop().run_sync(lambda: self.localize(pathdict))

      #complete the request only after localize completes
      if not success:
        raise HTTPError(500, "Error occurred during localization. See localization.log for details.")
      else:
        self.set_status(200)
        self.finish()

def load_jupyter_server_extension(nb_server_app):
  """Entrypoint for the Jupyter extension."""
  web_app = nb_server_app.web_app
  host_pattern = '.*$'
  route_pattern = url_path_join(web_app.settings['base_url'], '/api/localize')
  web_app.add_handlers(host_pattern, [(route_pattern, LocalizeHandler)])
  nb_server_app.log.info('initialized jupyter_localize_extension')
