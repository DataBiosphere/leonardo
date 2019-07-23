import copy
import datetime
import json
import os
import requests_mock
import shutil
import tempfile
import unittest
from datetime import timedelta
from nbformat.v4 import new_notebook
from tornado.testing import AsyncTestCase, gen_test
from unittest.mock import patch

# Import this first; see https://github.com/jupyter/notebook/issues/2798
import notebook.transutils
import jupyter_delocalize

class TestDelocalizingContentsManager(AsyncTestCase):
  """DelocalizingContentsManager tests"""

  def setUp(self):
    super(TestDelocalizingContentsManager, self).setUp()
    self.orig_ttl = jupyter_delocalize.METADATA_TTL
    jupyter_delocalize.METADATA_TTL = timedelta()
    self.manager = jupyter_delocalize.DelocalizingContentsManager(
        root_dir=tempfile.mkdtemp(),
        delete_to_trash=False
    )
    # Replaces gsutil with normal file commands.
    self.manager.file_cmd = []
    self.manager.new(model={'type': 'directory'}, path='dir')
    self.out_dir = tempfile.mkdtemp()

  def tearDown(self):
    jupyter_delocalize.METADATA_TTL = self.orig_ttl
    shutil.rmtree(self.manager.root_dir)
    shutil.rmtree(self.out_dir)
    super(TestDelocalizingContentsManager, self).tearDown()

  def _await_tornado(self):
    # We spawn the delocalize processes in a Tornado callback, which executes
    # asynchronously.
    self.io_loop.add_callback(self.stop)
    self.wait()

  def _save_new_notebook(self, path):
    content = new_notebook()
    self.manager.save({
        'type': 'notebook',
        'content': content,
        'format': 'text'
    }, path=path)
    self._await_tornado()
    return content.dict()

  def _save_delocalize_config(self, dir_path, config=None):
    if not config:
      config = {
          'destination': self.out_dir
      }
    self.manager.save({
        'type': 'file',
        'content': json.dumps(config),
        'format': 'text'
    }, path=dir_path + '/.delocalize.json')
    self._await_tornado()

  def _rename_file(self, from_path, to_path):
    self.manager.rename_file(from_path, to_path)
    self._await_tornado()

  def _delete_file(self, path):
    self.manager.delete_file(path)
    self._await_tornado()

  def test_save_normal(self):
    want = self._save_new_notebook('dir/foo.ipynb')
    self.assertEqual(os.listdir(self.out_dir), [])
    with open(self.manager.root_dir + '/dir/foo.ipynb', 'r') as got:
      self.assertEqual(json.load(got), want)

  def test_save_delocalize(self):
    self._save_delocalize_config('dir')
    want = self._save_new_notebook('dir/foo.ipynb')

    with open(self.out_dir + '/foo.ipynb', 'r') as got:
      self.assertEqual(json.load(got), want)
    with open(self.manager.root_dir + '/dir/foo.ipynb', 'r') as got:
      self.assertEqual(json.load(got), want)

  def test_save_delocalize_other_dirs(self):
    self.manager.new(model={'type': 'directory'}, path='dirA')
    self.manager.new(model={'type': 'directory'}, path='dir/dirB')
    self._save_delocalize_config('dir')

    self._save_new_notebook('foo.ipynb')
    self._save_new_notebook('dirA/fizz.ipynb')
    self._save_new_notebook('dir/dirB/bar.ipynb')
    self.assertEqual(os.listdir(self.out_dir), [])

  def test_save_delocalize_with_pattern(self):
    self._save_delocalize_config('.', config={
        'destination': self.out_dir,
        'pattern': '.*\.ipynb$'
    })

    self._save_new_notebook('foo.ipynb')
    self._save_new_notebook('falco.jpg')
    self._save_new_notebook('lombardi.pdf')
    self.assertEqual(os.listdir(self.out_dir), ['foo.ipynb'])

  def test_rename_normal(self):
    want = self._save_new_notebook('dir/foo.ipynb')
    self._rename_file('dir/foo.ipynb', 'dir/bar.ipynb')

    self.assertFalse(os.path.isfile(self.manager.root_dir + '/dir/foo.ipynb'))
    with open(self.manager.root_dir + '/dir/bar.ipynb', 'r') as got:
      self.assertEqual(json.load(got), want)

  def test_rename_delocalize(self):
    self._save_delocalize_config('dir')
    want = self._save_new_notebook('dir/foo.ipynb')
    self._rename_file('dir/foo.ipynb', 'dir/bar.ipynb')

    self.assertFalse(os.path.isfile(self.out_dir + '/foo.ipynb'))
    self.assertFalse(os.path.isfile(self.manager.root_dir + '/foo.ipynb'))
    with open(self.out_dir + '/bar.ipynb', 'r') as got:
      self.assertEqual(json.load(got), want)
    with open(self.manager.root_dir + '/dir/bar.ipynb', 'r') as got:
      self.assertEqual(json.load(got), want)

  def test_rename_delocalize_with_pattern(self):
    self._save_delocalize_config('dir', config={
        'destination': self.out_dir,
        'pattern': 'foo'
    })
    want = self._save_new_notebook('dir/foo.ipynb')
    self._rename_file('dir/foo.ipynb', 'dir/bar.ipynb')

    # The delocalization behavior for foo.ipynb in this case is unspecified;
    # currently it won't delete the file, but would be better if it did.
    self.assertFalse(os.path.isfile(self.out_dir + '/bar.ipynb'))
    self.assertFalse(os.path.isfile(self.manager.root_dir + '/foo.ipynb'))
    with open(self.manager.root_dir + '/dir/bar.ipynb', 'r') as got:
      self.assertEqual(json.load(got), want)

  def test_delete_normal(self):
    self._save_new_notebook('foo.ipynb')
    self._delete_file('foo.ipynb')

    self.assertFalse(os.path.isfile(self.manager.root_dir + '/foo.ipynb'))

  def test_delete_delocalize(self):
    self._save_delocalize_config('.')
    self._save_new_notebook('foo.ipynb')
    self._delete_file('foo.ipynb')

    self.assertFalse(os.path.isfile(self.out_dir + '/foo.ipynb'))
    self.assertFalse(os.path.isfile(self.manager.root_dir + '/foo.ipynb'))

  def test_delete_delocalize_with_pattern(self):
    self._save_delocalize_config('.')
    self._save_new_notebook('foo.ipynb')

    self._save_delocalize_config('.', config={
        'destination': self.out_dir,
        'pattern': 'doesnt match'
    })
    self._delete_file('foo.ipynb')

    self.assertTrue(os.path.isfile(self.out_dir + '/foo.ipynb'))
    self.assertFalse(os.path.isfile(self.manager.root_dir + '/foo.ipynb'))

  def test_metadata_cache(self):
    jupyter_delocalize.METADATA_TTL = timedelta(minutes=5)
    # Stub out and advance time manually.
    now = datetime.datetime(2018, 3, 20)
    self.manager._now = lambda: now
    self._save_new_notebook('foo.ipynb')
    self.assertEqual(os.listdir(self.out_dir), [])

    # A "not-found" should be cached, only 1 minute passed
    now += timedelta(minutes=1)
    self._save_delocalize_config('.')
    self._save_new_notebook('foo.ipynb')
    self.assertEqual(os.listdir(self.out_dir), [])

    # Cache TTL expired, will check again for delocalization config.
    now += timedelta(minutes=20)
    self._save_new_notebook('foo.ipynb')
    self.assertEqual(os.listdir(self.out_dir), ['foo.ipynb'])

class TestWelderContentsManager(AsyncTestCase):
  """WelderContentsManager tests"""

  def setUp(self):
    super(TestWelderContentsManager, self).setUp()
    self.manager = jupyter_delocalize.WelderContentsManager(
        root_dir=tempfile.mkdtemp(),
        delete_to_trash=False
    )
    self.manager.new(model={'type': 'directory'}, path='dir')

  def tearDown(self):
    shutil.rmtree(self.manager.root_dir)
    super(TestWelderContentsManager, self).tearDown()

  def _save_new_notebook(self, path):
    content = new_notebook()
    self.manager.save({
        'type': 'notebook',
        'content': content,
        'format': 'text'
    }, path=path)
    return content.dict()

  def _save_new_dir(self, path):
    self.manager.save({
        'type': 'directory',
        'content': '[]',
        'format': 'json'
    }, path=path)

  @requests_mock.mock()
  def test_save(self, mock_request):
    mock_request.post(self.manager.welder_base_url + '/objects')
    want = self._save_new_notebook('dir/foo.ipynb')
    with open(self.manager.root_dir + '/dir/foo.ipynb', 'r') as got:
      self.assertEqual(json.load(got), want)

  @requests_mock.mock()
  def test_save_dir(self, mock_request):
    self._save_new_dir('dir/foo')
    self.assertTrue(os.path.isdir(self.manager.root_dir + '/dir/foo'))

  @requests_mock.mock()
  def test_save_scratch_file(self, mock_request):
    mock_request.post(self.manager.welder_base_url + '/objects', status_code=412, json={
      'errorCode': 1
    })
    want = self._save_new_notebook('dir/foo.ipynb')
    with open(self.manager.root_dir + '/dir/foo.ipynb', 'r') as got:
      self.assertEqual(json.load(got), want)

  @requests_mock.mock()
  def test_save_new_file_reverts_on_fail(self, mock_request):
    mock_request.post(self.manager.welder_base_url + '/objects', status_code=412, json={
      'errorCode': 4
    })
    try:
      self._save_new_notebook('dir/foo.ipynb')
      self.fail('expected error on save')
    except IOError:
      pass
    self.assertFalse(os.path.isfile(self.manager.root_dir + '/dir/foo.ipynb'))

  @requests_mock.mock()
  def test_save_reverts_on_fail(self, mock_request):
    mock_request.post(self.manager.welder_base_url + '/objects')
    content = new_notebook()
    self.manager.save({
        'type': 'notebook',
        'content': content,
        'format': 'text'
    }, path='dir/foo.ipynb')

    mock_request.post(self.manager.welder_base_url + '/objects', status_code=412)
    updated_content = copy.deepcopy(content)
    updated_content['cells'] = [{
        'cell_type': 'markdown',
        'metadata': {},
        'source': ['XD'],
    }]
    try:
      self.manager.save({
          'type': 'notebook',
          'content': updated_content,
          'format': 'text'
      }, path='dir/foo.ipynb')
      self.fail('expected error on save')
    except IOError:
      pass
    with open(self.manager.root_dir + '/dir/foo.ipynb', 'r') as got:
      self.assertEqual(json.load(got)['cells'], content.dict()['cells'])

  def mock_edit_mode_meta(self, mock_request, edit_mode=True):
    if not edit_mode:
      mock_request.post(self.manager.welder_base_url + '/objects/metadata', status_code=412)
    else:
      mock_request.post(self.manager.welder_base_url + '/objects/metadata', json={
        'syncMode': 'EDIT'
      })

  @requests_mock.mock()
  def test_delete(self, mock_request):
    self.mock_edit_mode_meta(mock_request)
    mock_request.post(self.manager.welder_base_url + '/objects')
    self._save_new_notebook('dir/foo.ipynb')

    self.manager.delete_file('dir/foo.ipynb')
    self.assertFalse(os.path.isfile(self.manager.root_dir + '/dir/foo.ipynb'))

  @requests_mock.mock()
  def test_delete_empty_dir(self, mock_request):
    self.mock_edit_mode_meta(mock_request)
    mock_request.post(self.manager.welder_base_url + '/objects')
    self._save_new_dir('dir/foo')

    self.manager.delete_file('dir/foo')
    self.assertFalse(os.path.isdir(self.manager.root_dir + '/dir/foo'))

  @requests_mock.mock()
  def test_delete_dir_with_notebook(self, mock_request):
    self.mock_edit_mode_meta(mock_request)
    mock_request.post(self.manager.welder_base_url + '/objects')
    self._save_new_dir('dir/foo')
    self._save_new_notebook('dir/foo/nb.ipynb')

    try:
      self.manager.delete_file('dir/foo')
      self.fail('expected error on non-empty edit mode directory deletion')
    except NotImplementedError:
      pass
    self.assertTrue(os.path.isdir(self.manager.root_dir + '/dir/foo'))

  @requests_mock.mock()
  def test_delete_scratch_file(self, mock_request):
    self.mock_edit_mode_meta(mock_request, edit_mode=False)
    mock_request.post(self.manager.welder_base_url + '/objects')
    self._save_new_notebook('dir/foo.ipynb')

    self.manager.delete_file('dir/foo.ipynb')
    self.assertFalse(os.path.isfile(self.manager.root_dir + '/dir/foo.ipynb'))

  @requests_mock.mock()
  def test_delete_scratch_dir(self, mock_request):
    self.mock_edit_mode_meta(mock_request, edit_mode=False)
    self._save_new_dir('dir/foo')

    self.manager.delete_file('dir/foo')
    self.assertFalse(os.path.isdir(self.manager.root_dir + '/dir/foo'))

  @requests_mock.mock()
  def test_delete_local_file_survives_welder_error(self, mock_request):
    self.mock_edit_mode_meta(mock_request)
    mock_request.post(self.manager.welder_base_url + '/objects')
    self._save_new_notebook('dir/foo.ipynb')

    mock_request.post(self.manager.welder_base_url + '/objects', status_code=412, json={
      'errorCode': 4
    })
    try:
      self.manager.delete_file('dir/foo.ipynb')
      self.fail('expected error on delete')
    except IOError:
      pass
    self.assertTrue(os.path.isfile(self.manager.root_dir + '/dir/foo.ipynb'))

  @requests_mock.mock()
  def test_rename(self, mock_request):
    post_mock = mock_request.post(self.manager.welder_base_url + '/objects')
    self.mock_edit_mode_meta(mock_request)
    want = self._save_new_notebook('dir/foo.ipynb')

    # Creating the initial notebook above results in a Welder post.
    posts_before_rename = post_mock.call_count
    self.manager.rename('dir/foo.ipynb', 'dir/bar.ipynb')
    self.assertFalse(os.path.isfile(self.manager.root_dir + '/dir/foo.ipynb'))
    with open(self.manager.root_dir + '/dir/bar.ipynb', 'r') as got:
      self.assertEqual(json.load(got), want)

    self.assertEqual(post_mock.call_count - posts_before_rename, 2)

  @requests_mock.mock()
  def test_rename_empty_dir(self, mock_request):
    self.mock_edit_mode_meta(mock_request)
    mock_request.post(self.manager.welder_base_url + '/objects')
    self._save_new_dir('dir/foo')

    self.manager.rename('dir/foo', 'dir/bar')
    self.assertFalse(os.path.isdir(self.manager.root_dir + '/dir/foo'))
    self.assertTrue(os.path.isdir(self.manager.root_dir + '/dir/bar'))

  @requests_mock.mock()
  def test_rename_dir_with_notebook(self, mock_request):
    post_mock = mock_request.post(self.manager.welder_base_url + '/objects')
    self.mock_edit_mode_meta(mock_request)
    self._save_new_dir('dir/foo')
    self._save_new_notebook('dir/foo/nb.ipynb')

    try:
      self.manager.rename('dir/foo', 'dir/bar')
      self.fail('expected error on non-empty edit mode rename')
    except:
      pass
    self.assertTrue(os.path.isdir(self.manager.root_dir + '/dir/foo'))
    self.assertFalse(os.path.isdir(self.manager.root_dir + '/dir/bar'))

  @requests_mock.mock()
  def test_rename_scratch_file(self, mock_request):
    post_mock = mock_request.post(self.manager.welder_base_url + '/objects')
    self.mock_edit_mode_meta(mock_request, edit_mode=False)
    want = self._save_new_notebook('dir/foo.ipynb')

    # Creating the initial notebook above results in a Welder post.
    posts_before_rename = post_mock.call_count
    self.manager.rename('dir/foo.ipynb', 'dir/bar.ipynb')
    self.assertFalse(os.path.isfile(self.manager.root_dir + '/dir/foo.ipynb'))
    with open(self.manager.root_dir + '/dir/bar.ipynb', 'r') as got:
      self.assertEqual(json.load(got), want)

    self.assertEqual(post_mock.call_count - posts_before_rename, 0)

  @requests_mock.mock()
  def test_rename_scratch_dir(self, mock_request):
    self.mock_edit_mode_meta(mock_request, edit_mode=False)
    self._save_new_dir('dir/foo')

    self.manager.rename('dir/foo', 'dir/bar')
    self.assertFalse(os.path.isdir(self.manager.root_dir + '/dir/foo'))
    self.assertTrue(os.path.isdir(self.manager.root_dir + '/dir/bar'))

  def _delete_req_matcher(self, path):
    def m(req):
      return req.json()['action'] == 'delete' and req.json()['localPath'] == path
    return m

  @requests_mock.mock()
  def test_rename_cleanup_on_delete_fail(self, mock_request):
    mock_request.post(self.manager.welder_base_url + '/objects')
    mock_request.post(self.manager.welder_base_url + '/objects', additional_matcher=self._delete_req_matcher('dir/foo.ipynb'), status_code=500)
    mock_request.post(self.manager.welder_base_url + '/objects', additional_matcher=self._delete_req_matcher('dir/bar.ipynb'))
    self.mock_edit_mode_meta(mock_request)
    want = self._save_new_notebook('dir/foo.ipynb')

    # Creating the initial notebook above results in a Welder post.
    try:
      self.manager.rename('dir/foo.ipynb', 'dir/bar.ipynb')
      self.fail('expected rename exception')
    except IOError:
      pass

    self.assertFalse(os.path.isfile(self.manager.root_dir + '/dir/bar.ipynb'))
    with open(self.manager.root_dir + '/dir/foo.ipynb', 'r') as got:
      self.assertEqual(json.load(got), want)

if __name__ == '__main__':
    unittest.main()
