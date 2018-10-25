import distutils.util
import subprocess
import os
import tornado
from tornado import gen
from tornado.web import HTTPError
from notebook.base.handlers import IPythonHandler
from notebook.utils import url_path_join
from datauri import DataURI

class LocalizeHandler(IPythonHandler):
  def _sanitize(self, pathstr):
    """Sanitizes paths. Handles local paths, gs: URIs, and data: URIs."""
    # return gs or data uris as is
    if pathstr.startswith("gs:") or pathstr.startswith("data:"):
      return pathstr
    # expand user directories and make intermediate directories
    else:
      expanded = os.path.expanduser(pathstr)
      try:
        os.makedirs(os.path.dirname(expanded))
      except OSError: #thrown if dirs already exist
        pass
      return expanded

  def _localize_gcs_uri(self, locout, source, dest):
    """Localizes an entry where either the source or destination is a gs: path.
    Simply invokes gsutil in a subprocess."""

    # Use a sequence of arguments with Shell=False. The subprocess module takes care
    # of quoting/escaping arguments. See:
    #   https://docs.python.org/2/library/subprocess.html#subprocess.call
    #   https://docs.python.org/2/library/subprocess.html#frequently-used-arguments
    cmd = ['gsutil', '-m', '-q', 'cp', '-R', '-c', '-e', source, dest]
    locout.write(' '.join(cmd) + '\n')
    result = subprocess.call(cmd, stderr=locout)
    return result == 0

  def check_gcs_object_Status(self, locout, source, dest):
    source_check = ['gsutil', '-m', '-q', 'ls', source]
    locout.write(' '.join(source_check) + '\n')
    source_status = subprocess.call(source_check, stderr=locout)
    dest_check = ['gsutil', '-m', '-q', 'ls', dest]
    locout.write(' '.join(dest_check) + '\n')
    dest_status = subprocess.call(dest_check, stderr=locout)
    return source_status == 0 and dest_status == 0

  def _localize_data_uri(self, locout, source, dest):
    """Localizes an entry where the source is a data: URI"""
    try:
      uri = DataURI(source)
    except ValueError:
      locout.write('Could not parse "{}" as a data URI: {}\n'.format(source, str(e)))
      return False

    try:
      with open(dest, 'w+', buffering=1) as destout:
        destout.write(uri.data)
        locout.write('{}: wrote {} bytes\n'.format(dest, len(uri.data)))
    except IOError as e:
      locout.write('{}: I/O error({0}): {1}\n'.format(dest, e.errno, e.strerror))
      return False
    except:
      locout.write('{}: unexpected error: {}\n'.format(sys.exc_info()[0]))
      return False

    return True

  @gen.coroutine
  def localize(self, pathdict):
    """Treats the given dict as a string/string map and localizes each entry one by one.
    Returns a list of any failed entries."""
    failures = []
    #This gets dropped inside the user's notebook working directory
    with open("localization.log", 'a', buffering=1) as locout:
      for key in pathdict:
        #NOTE: keys are destinations, values are sources
        source = self._sanitize(pathdict[key])
        dest = self._sanitize(key)

        if source.startswith('gs:') or dest.startswith('gs:'):
          status = self.check_gcs_object_Status(locout, source, dest)
          if status:
            success = self._localize_gcs_uri(locout, source, dest)
          else:
            locout.write('Could not validate source or destination: {} -> {}.\n'.format(source, dest))
            success = False
        elif source.startswith('data:'):
          success = self._localize_data_uri(locout, source, dest)
        else:
          locout.write('Unhandled localization entry: {} -> {}. Required gs: or data: URIs.\n'.format(source, dest))
          success = False

        if not success:
          failures.append((dest, source))

    return failures

  def post(self):
    try:
      pathdict = tornado.escape.json_decode(self.request.body)
    except ValueError:
      raise HTTPError(400, "Body must be JSON object of type string/string")

    if type(pathdict) is not dict:
      raise HTTPError(400, "Body must be JSON object of type string/string")

    if not all(map(lambda v: type(v) is unicode, pathdict.values())):
      raise HTTPError(400, "Body must be JSON object of type string/string")

    try:
      raw = self.get_query_argument('async', 'false')
      async = distutils.util.strtobool(raw)
    except ValueError:
      raise HTTPError(400, "Could not parse async parameter as a boolean: '{}'".format(raw))

    if async:
      #complete the request HERE, without waiting for the localize to run
      self.set_status(200)
      self.finish()

      #fire and forget the actual work -- it'll log to a file in the user's homedir
      tornado.ioloop.IOLoop.current().spawn_callback(self.localize, pathdict)

    else:
      #run localize synchronous to the HTTP request
      #run_sync() doesn't take arguments, so we must wrap the call in a lambda.
      failures = tornado.ioloop.IOLoop().run_sync(lambda: self.localize(pathdict))

      #complete the request only after localize completes
      if failures:
        raise HTTPError(500, "Error occurred localizing the following {} entries: {}. See localization.log for details.".format(
          len(failures), str(failures)))
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
