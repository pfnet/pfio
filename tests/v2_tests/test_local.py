import io
import json
import os
import pickle
import tempfile
import unittest
from collections.abc import Iterable

import pytest

from pfio.v2 import Local, LocalFileStat, from_url, open_url


class TestLocal(unittest.TestCase):

    def setUp(self):
        self.test_string_str = "this is a test string\n"
        self.test_string_bytes = self.test_string_str.encode("utf-8")

        self.testdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.testdir.cleanup()

    def test_repr_str(self):
        with Local(self.testdir.name) as fs:
            str(fs)
            repr(fs)

    def test_read_string(self):

        with Local() as fs:
            with tempfile.NamedTemporaryFile("w+t", delete=False) as tmpfile:
                tmpfile_path = tmpfile.name
                tmpfile.write(self.test_string_str)

            with fs.open(tmpfile_path, mode="r") as loaded_file:
                self.assertEqual(self.test_string_str, loaded_file.read())

            with fs.open(tmpfile_path, mode="r") as loaded_file:
                self.assertEqual(self.test_string_str, loaded_file.readline())

            fs.remove(tmpfile_path)

    def test_read_bytes(self):

        with Local() as fs:
            with tempfile.NamedTemporaryFile("w+b", delete=False) as tmpfile:
                tmpfile_path = tmpfile.name
                tmpfile.write(self.test_string_bytes)

            with fs.open(tmpfile_path, mode="rb") as loaded_file:
                self.assertEqual(self.test_string_bytes, loaded_file.read())

            fs.remove(tmpfile_path)

    def test_open_non_exist(self):

        non_exist_file = "non_exist_file.txt"
        if os.path.exists(non_exist_file):
            os.remove(non_exist_file)

        with Local() as fs:
            self.assertRaises(IOError, fs.open, non_exist_file)

    def test_list(self):
        # directory layout
        # testlsdir
        # | - nested_dir1
        # |   | - nested_dir3
        # | _ nested_dir2
        for test_dir_name in ["testlsdir", "testlsdir/"]:
            try:
                tmpdir = tempfile.TemporaryDirectory()
                nested_dir_name1 = "nested_dir1"
                nested_dir_name2 = "nested_dir2"
                nested_dir_name3 = "nested_dir3"
                test_dir_path = os.path.join(tmpdir.name, test_dir_name)
                nested_dir_path1 = os.path.join(test_dir_path,
                                                nested_dir_name1)
                nested_dir_path2 = os.path.join(test_dir_path,
                                                nested_dir_name2)
                nested_dir_path3 = os.path.join(nested_dir_path1,
                                                nested_dir_name3)
                nested_dir_relative_path3 = os.path.join(nested_dir_name1,
                                                         nested_dir_name3)

                with Local() as fs:
                    fs.makedirs(nested_dir_path1)
                    fs.makedirs(nested_dir_path2)
                    fs.makedirs(nested_dir_path3)

                    self.assertIsInstance(fs.list(), Iterable)
                    full_list = list(fs.list(test_dir_path, recursive=True))
                    self.assertIn(nested_dir_name1+'/', full_list)
                    self.assertIn(nested_dir_name2+'/', full_list)
                    self.assertIn(nested_dir_relative_path3+'/', full_list)

                    first_level_list_of_file = list(fs.list(
                        test_dir_path))
                    self.assertIn(nested_dir_name1+'/',
                                  first_level_list_of_file)
                    self.assertIn(nested_dir_name2+'/',
                                  first_level_list_of_file)
                    self.assertNotIn(nested_dir_relative_path3+'/',
                                     first_level_list_of_file)
            finally:
                tmpdir.cleanup()

    def test_isdir(self):
        with Local() as fs:
            self.assertTrue(fs.isdir("/"))
            self.assertFalse(fs.isdir("test_posix_handler.py"))

    def test_mkdir(self):
        test_dir_name = "testmkdir/"
        with Local(self.testdir.name) as fs:
            fs.mkdir(test_dir_name)
            self.assertTrue(fs.isdir(test_dir_name))

            fs.remove(test_dir_name)

    def test_makedirs(self):
        test_dir_name = "testmkdir/"
        nested_dir_name = test_dir_name + "nested_dir"

        with Local(self.testdir.name) as fs:
            fs.makedirs(nested_dir_name)
            self.assertTrue(fs.isdir(nested_dir_name))

            fs.remove(test_dir_name, True)

    def test_picle(self):

        pickle_file_name = "test_pickle.pickle"
        test_data = {'test_elem1': b'balabala',
                     'test_elem2': 'balabala'}

        with Local(self.testdir.name) as fs:
            with fs.open(pickle_file_name, 'wb') as f:
                pickle.dump(test_data, f)
            with fs.open(pickle_file_name, 'rb') as f:
                loaded_obj = pickle.load(f)
                self.assertEqual(test_data, loaded_obj)

            fs.remove(pickle_file_name)

    def test_exists(self):
        non_exist_file = "non_exist_file.txt"

        with Local() as fs:
            self.assertTrue(fs.exists(__file__))
            self.assertTrue(fs.exists("/"))
            self.assertFalse(fs.exists(non_exist_file))

    def test_rename(self):
        with Local(self.testdir.name) as fs:
            with fs.open('src', 'w') as fp:
                fp.write('foobar')

            self.assertTrue(fs.exists('src'))
            self.assertFalse(fs.exists('dst'))

            fs.rename('src', 'dst')
            self.assertFalse(fs.exists('src'))
            self.assertTrue(fs.exists('dst'))

            with fs.open('dst', 'r') as fp:
                data = fp.read()
                assert data == 'foobar'

            fs.remove('dst')

    def test_remove(self):
        test_file = "test_remove.txt"
        test_dir = "test_dir/"
        nested_dir = os.path.join(test_dir, "nested_file/")
        nested_file = os.path.join(nested_dir, test_file)

        with Local(self.testdir.name) as fs:
            with fs.open(test_file, 'w') as fp:
                fp.write('foobar')

            # test remove on one file
            self.assertTrue(fs.exists(test_file))
            fs.remove(test_file)
            self.assertFalse(fs.exists(test_file))

            # test remove on directory
            fs.makedirs(nested_dir)
            with fs.open(nested_file, 'w') as fp:
                fp.write('foobar')

            self.assertTrue(fs.exists(test_dir))
            self.assertTrue(fs.exists(nested_dir))
            self.assertTrue(fs.exists(nested_file))

            fs.remove(test_dir, True)

            self.assertFalse(fs.exists(test_dir))
            self.assertFalse(fs.exists(nested_dir))
            self.assertFalse(fs.exists(nested_file))

    def test_stat_file(self):
        test_file_name = "testfile"
        with Local(self.testdir.name) as fs:
            with fs.open(test_file_name, 'w') as fp:
                fp.write('foobar')

            expected = os.stat(os.path.join(fs.cwd, test_file_name))

            stat = fs.stat(test_file_name)
            self.assertIsInstance(stat, LocalFileStat)
            self.assertTrue(stat.filename.endswith(test_file_name))
            self.assertFalse(stat.isdir())
            self.assertIsInstance(stat.last_accessed, float)
            self.assertIsInstance(stat.last_modified, float)
            self.assertIsInstance(stat.created, float)
            keys = (('last_modified', 'st_mtime'),
                    ('last_accessed', 'st_atime'),
                    ('last_modified_ns', 'st_mtime_ns'),
                    ('last_accessed_ns', 'st_atime_ns'),
                    ('created', 'st_ctime'), ('created_ns', 'st_ctime_ns'),
                    ('mode', 'st_mode'), ('size', 'st_size'),
                    ('uid', 'st_uid'), ('gid', 'st_gid'), ('ino', 'st_ino'),
                    ('dev', 'st_dev'), ('nlink', 'st_nlink'))
            for k, kexpect in keys:
                self.assertEqual(getattr(stat, k), getattr(expected, kexpect))

            fs.remove(test_file_name)

    def test_stat_directory(self):
        test_dir_name = "testmkdir"
        with Local(self.testdir.name) as fs:
            fs.mkdir(test_dir_name)

            expected = os.stat(os.path.join(self.testdir.name, test_dir_name))

            stat = fs.stat(test_dir_name)
            self.assertIsInstance(stat, LocalFileStat)
            self.assertTrue(stat.filename.endswith(test_dir_name))
            self.assertTrue(stat.isdir())
            self.assertIsInstance(stat.last_accessed, float)
            self.assertIsInstance(stat.last_modified, float)
            self.assertIsInstance(stat.created, float)
            keys = (('last_modified', 'st_mtime'),
                    ('last_accessed', 'st_atime'),
                    ('last_modified_ns', 'st_mtime_ns'),
                    ('last_accessed_ns', 'st_atime_ns'),
                    ('created', 'st_ctime'), ('created_ns', 'st_ctime_ns'),
                    ('mode', 'st_mode'), ('size', 'st_size'),
                    ('uid', 'st_uid'), ('gid', 'st_gid'), ('ino', 'st_ino'),
                    ('dev', 'st_dev'), ('nlink', 'st_nlink'))
            for k, kexpect in keys:
                self.assertEqual(getattr(stat, k), getattr(expected, kexpect))

            fs.remove(test_dir_name)

    def test_fs_factory(self):
        with Local(self.testdir.name) as fs:
            with fs.open('foo.txt', 'w') as fp:
                fp.write('bar')

            fs.makedirs('nested_dir')
            with fs.open('nested_dir/hello.txt', 'w') as fp:
                fp.write('world')

        with from_url(self.testdir.name) as fs:
            assert isinstance(fs, Local)
            assert fs.exists('foo.txt')
            assert fs.isdir('nested_dir')
            with fs.open('foo.txt', 'r') as f:
                assert f.read() == 'bar'
            with fs.open('nested_dir/hello.txt', 'r') as f:
                assert f.read() == 'world'

        with from_url(os.path.join(self.testdir.name, 'nested_dir')) as fs:
            assert isinstance(fs, Local)
            assert fs.exists('hello.txt')
            with fs.open('hello.txt', 'r') as f:
                assert f.read() == 'world'

        with open_url(os.path.join(self.testdir.name, 'foo.txt'), 'rt') as f:
            assert isinstance(f, io.TextIOWrapper)
            assert f.read() == 'bar'

    def test_from_url_create_option(self):
        path = os.path.join(self.testdir.name, 'non-existent-directory')
        with pytest.raises(ValueError):
            from_url(path)

        from_url(path, create=True)
        assert os.path.exists(path) and os.path.isdir(path)


def test_local_rw_profiling():
    ppe = pytest.importorskip("pytorch_pfn_extras")

    with Local(trace=True) as fs:
        ppe.profiler.clear_tracer()

        with fs.open('foo.txt', 'w') as fp:
            fp.write('bar')

        dict = ppe.profiler.get_tracer().state_dict()
        keys = [event["name"] for event in json.loads(dict['_event_list'])]

        assert "pfio.v2.Local:open" in keys
        assert "pfio.v2.Local:write" in keys

    with Local(trace=True) as fs:
        ppe.profiler.clear_tracer()

        with fs.open('foo.txt', 'r') as fp:
            tmp = fp.read()
            assert tmp == 'bar'

        dict = ppe.profiler.get_tracer().state_dict()
        keys = [event["name"] for event in json.loads(dict['_event_list'])]

        assert "pfio.v2.Local:open" in keys
        assert "pfio.v2.Local:read" in keys

    # no profile case
    with Local(trace=False) as fs:
        ppe.profiler.clear_tracer()

        with fs.open('foo.txt', 'r') as fp:
            tmp = fp.read()
            assert tmp == 'bar'

        dict = ppe.profiler.get_tracer().state_dict()
        keys = [event["name"] for event in json.loads(dict['_event_list'])]

        assert "pfio.v2.Local:open" not in keys
        assert "pfio.v2.Local:read" not in keys


if __name__ == '__main__':
    unittest.main()
