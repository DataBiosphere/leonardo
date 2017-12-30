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
        os.makedirs(expanded)
      except OSError: #thrown if dirs already exist
        pass
    return pipes.quote(expanded)

  @gen.coroutine
  def localize(self, pathdict):
    """Treats the given dict as a string/string map and sends it to gsutil."""
    #This gets dropped inside the user's notebook working directory
    with open("localization.log", 'a') as locout:
      for key in pathdict:
        #NOTE: keys are destinations, values are sources
        cmd = " ".join(["gsutil -m -q cp -R", self.sanitize(pathdict[key]), self.sanitize(key)])
        locout.write(cmd + '\n')
        subprocess.call(cmd, stderr=locout, shell=True)

  def post(self):
    try:
      pathdict = tornado.escape.json_decode(self.request.body)
    except ValueError:
      raise HTTPError(400, "Body must be JSON object of type string/string")

    if type(pathdict) is not dict:
      raise HTTPError(400, "Body must be JSON object of type string/string")

    if not all(map(lambda v: type(v) is unicode, pathdict.values())):
      raise HTTPError(400, "Body must be JSON object of type string/string")

    #complete the request HERE, without waiting for the localize to run
    self.set_status(200)
    self.finish()

    #fire and forget the actual work -- it'll log to a file in the user's homedir
    tornado.ioloop.IOLoop.current().spawn_callback(self.localize, pathdict)

def load_jupyter_server_extension(nb_server_app):
  """Entrypoint for the Jupyter extension."""
  web_app = nb_server_app.web_app
  host_pattern = '.*$'
  route_pattern = url_path_join(web_app.settings['base_url'], '/api/localize')
  web_app.add_handlers(host_pattern, [(route_pattern, LocalizeHandler)])
