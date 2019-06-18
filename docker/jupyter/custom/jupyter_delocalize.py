from datetime import datetime, timedelta
import json
import os
import re
import requests
import subprocess
import tornado
from notebook.services.contents.filemanager import FileContentsManager

METADATA_TTL = timedelta(minutes=5)

class DelocalizingContentsManager(FileContentsManager):
  """
  A contents manager allowing for configurable automated delocalization.

  Files will always be persisted to the local Jupyter file system as usual.
  Delocalization may occur for a given file operation only if there exists a
  file named ".delocalize.json" within the same directory as that file on the
  Jupyter file system. The format of this JSON is as follows:

  {
    // Optional, only delocalize files from the collocated directory with a
    // basename matching this Python "re" regex. If unspecified, all collocated
    // files will be matched.
    "pattern": "",
    // Required. Cloud storage directory in which to persist matching files.
    "destination": ""
  }

  gsutil is used to delocalize files. A log is produced on the Jupyter server
  in the root directory. As a rule, this logfile should only be created if
  delocalization is configured in at least one subdirectory.

  Note: when a new .delocalize.json config is added, existing eligible files
  will not be automatically backfilled. They will only be delocalized upon
  further modification.
  """

  # TODO:
  # - Support GCS version preconditions on save
  # - Support a recursive option in .delocalize.json
  # - Invalidate the cache when a new .delocalize.json file is written

  # Cache delocalization metadata for 5 minutes. The cache points to a tuple of
  # (metadata: dict, read_at: datetime), with a key of JSON file path. Negative
  # lookups are stored here as well with None as metadata (this will be the common
  # case for any clusters which don't use delocalization).
  def __init__(self, *args, **kwargs):
    self.log.info('initializing DelocalizingContentsManager')
    self.delocalize_metadata = {}
    # Allows for stubbing in tests.
    self.file_cmd = ['gsutil', '-q', '-m']
    super(DelocalizingContentsManager, self).__init__(*args, **kwargs)

  def _now(self):
    """Current time, stubbed for testing"""
    return datetime.now()

  def _find_delocalize_meta(self, path):
    now = self._now()
    dir_name = os.path.dirname(path)
    json_path = os.path.join(dir_name, '.delocalize.json')
    if json_path in self.delocalize_metadata:
      (meta, read_at) = self.delocalize_metadata[json_path]
      if now - read_at < METADATA_TTL:
        return meta
      # Cache expiry.
      del self.delocalize_metadata[json_path]
    try:
      with open(json_path, 'r') as f:
        # TODO: Support recursive application of delocalize configs.
        config = json.load(f)
        if 'destination' not in config:
          raise ValueError('.delocalize.json is missing required "destination"')
        if config['destination'].startswith(self.root_dir):
          raise ValueError(
              'delocalizing to the Jupyter root dir "{}" is illegal'.format(self.root_dir))
        if 'pattern' in config:
          try:
            config['pattern'] = re.compile(config['pattern'])
          except:
            raise ValueError('invalid pattern: "{}"'.format(config['pattern']))
        self.delocalize_metadata[json_path] = (config, now)
    except IOError as e:
      self.delocalize_metadata[json_path] = (None, now)
    except ValueError as e:
      self.delocalize_metadata[json_path] = (None, now)
      with open('delocalization.log', 'a', buffering=1) as locout:
        locout.write(dir_name + ': ' + str(e) + '\n')

    return self.delocalize_metadata[json_path][0]

  def _should_skip_file(self, meta, os_path):
    base_name = os.path.basename(os_path)
    if base_name == '.delocalize.json':
      return True
    if 'pattern' in meta and not meta['pattern'].search(base_name):
      self.log.info(
          'skipping delocalize for "{}", doesn\'t match pattern "{}"'.format(
              base_name, meta['pattern'].pattern))
      return True
    return False

  def _remote_path(self, meta, os_path):
    return meta['destination'] + '/' + os.path.basename(os_path)

  @tornado.gen.coroutine
  def _log_and_call_file_cmd(self, args):
    with open('delocalization.log', 'a', buffering=1) as locout:
      cmd = self.file_cmd + args
      locout.write(' '.join(cmd) + '\n')
      subprocess.call(cmd, stderr=locout)

  def _log_and_call_file_cmd_async(self, args):
    tornado.ioloop.IOLoop.current().spawn_callback(
        self._log_and_call_file_cmd, args)

  def save(self, model, path=''):
    ret = super(DelocalizingContentsManager, self).save(model, path)
    if not path or model['type'] == 'directory':
      return ret
    # Sometimes the "path" contains a leading /, sometimes not; let Jupyter convert.
    os_path = self._get_os_path(path)
    meta = self._find_delocalize_meta(os_path)
    if not meta or self._should_skip_file(meta, os_path):
      return ret

    self._log_and_call_file_cmd_async(
        ['cp', os_path, self._remote_path(meta, os_path)])
    return ret

  def rename_file(self, old_path, new_path):
    super(DelocalizingContentsManager, self).rename_file(old_path, new_path)
    old_os_path = self._get_os_path(old_path)
    new_os_path = self._get_os_path(new_path)
    old_meta = self._find_delocalize_meta(old_os_path)
    new_meta = self._find_delocalize_meta(new_os_path)
    if (not old_meta or not new_meta or
        self._should_skip_file(old_meta, old_os_path) or
        self._should_skip_file(new_meta, new_os_path)):
      # TODO: Could improve handling of edge cases here, i.e. delete if moving
      # from configured -> non-configured or create on the converse. Unclear
      # whether this operation is even supported in the Jupyter UI.
      return

    self._log_and_call_file_cmd_async([
        'mv',
        self._remote_path(old_meta, old_os_path),
        self._remote_path(new_meta, new_os_path)
    ])

  def delete_file(self, path):
    super(DelocalizingContentsManager, self).delete_file(path)
    os_path = self._get_os_path(path)
    meta = self._find_delocalize_meta(os_path)
    if not meta or self._should_skip_file(meta, os_path):
      return

    self._log_and_call_file_cmd_async([
        'rm', self._remote_path(meta, os_path),
    ])


class WelderContentsManager(FileContentsManager):
  """
  A contents manager which integrates with the Leo Welder service.

  Blocking Welder API calls are made before files are persisted. After a
  successful call to Welder, files are persisted to the local Jupyter file
  system as usual.
  """

  def __init__(self, *args, **kwargs):
    # This log line shouldn't be necessary, but Jupyter's built-in logging is
    # lacking and its configuration can be complex. Having this in the server
    # logs is useful for confirming which ContentsManager is in use.
    self.log.info('initializing WelderContentsManager')
    super(WelderContentsManager, self).__init__(*args, **kwargs)

  def _welder_delocalize(self, path):
    # Ignore storage link failure, throw other errors.
    resp = requests.post('http://localhost:8080/objects', data=json.dumps({
      'action': 'safeDelocalize',
      # Sometimes the Jupyter UI provided "path" contains a leading /, sometimes
      # not; strip for Welder.
      'localPath': path.lstrip('/')
    }))
    if not resp.ok:
      try:
        msg = json.dumps(resp.json())
      except:
        msg = resp.reason
      raise IOError("safeDelocalize failed: " + msg)

  def save(self, model, path=''):
    ret = super(WelderContentsManager, self).save(model, path)
    if not path or model['type'] == 'directory':
      return ret

    # TODO(calbach): Consider passing file contents directly to avoid this revert.
    orig_model = self.get(path)
    try:
      self._welder_delocalize(path)
    except Exception as werr:
      self.log.info("welder save failed, attempting to revert local file: " + str(werr))
      try:
        super(WelderContentsManager, self).save(orig_model, path)
      except Exception as rerr:
        self.log.severe("failed to revert after Welder error, local disk is in an inconsistent state: " + str(rerr))
      raise werr
    return ret

  def rename_file(self, old_path, new_path):
    # TODO(IA-1028): Integrate Welder renaming.
    super(WelderContentsManager, self).rename_file(old_path, new_path)

  def delete_file(self, path):
    # TODO(IA-1049): Integrate Welder delete.
    super(WelderContentsManager, self).delete_file(path)
