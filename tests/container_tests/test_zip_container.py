import unittest

import pfio

import io
import multiprocessing
import os
import pickle
import pytest
import random
import shutil
import string
import sys
import tempfile
from zipfile import ZipFile
from datetime import datetime
import subprocess
from parameterized import parameterized


def make_zip(zipfilename, root_dir, base_dir):
    pwd = os.getcwd()
    with ZipFile(zipfilename, "w") as f:
        os.chdir(root_dir)
        for root, dirs, filenames in os.walk(base_dir):
            for _dir in dirs:
                path = os.path.normpath(os.path.join(root, _dir))
                f.write(path)
            for _file in filenames:
                path = os.path.normpath(os.path.join(root, _file))
                f.write(path)
        os.chdir(pwd)


def make_random_str(n):
    return ''.join([random.choice(string.ascii_letters + string.digits)
                    for i in range(n)])


ZIP_TEST_FILENAME_LIST = {
    "dir_name1": "testdir1",
    "dir_name2": "testdir2",
    "zipped_file_name": "testfile1",
    "testfile_name": "testfile2",
    "nested_dir_name": "nested_dir",
    "nested_zip_file_name": "nested.zip",
}

NON_EXIST_LIST = ["does_not_exist", "does_not_exist/", "does/not/exist"]


class TestZipHandler(unittest.TestCase):

    def setUp(self):
        # The following zip layout is created for all the tests
        # outside.zip
        # | - testdir1
        # |   | - nested1.zip
        # |       | - nested_dir
        # |           | - nested
        # | - testdir2
        # |   | - testfile1
        # | - testfile2
        self.test_string = "this is a test string\n"
        self.nested_test_string = \
            "this is a test string for nested zip\n"
        self.test_string_b = self.test_string.encode("utf-8")
        self.nested_test_string_b = \
            self.nested_test_string.encode("utf-8")
        self.fs_handler = pfio.create_handler("posix")

        # the most outside zip
        self.zip_file_name = "outside"

        # nested zip and nested file
        self.tmpdir = tempfile.TemporaryDirectory()
        self.nested_zipped_file_name = "nested"
        self.nested_dir_name = ZIP_TEST_FILENAME_LIST["nested_dir_name"]
        self.nested_dir_path = os.path.join(self.tmpdir.name,
                                            self.nested_dir_name)
        self.nested_zip_file_name = \
            ZIP_TEST_FILENAME_LIST["nested_zip_file_name"]

        # directory and file
        self.dir_name1 = ZIP_TEST_FILENAME_LIST["dir_name1"]
        self.dir_name2 = ZIP_TEST_FILENAME_LIST["dir_name2"]
        self.zipped_file_name = ZIP_TEST_FILENAME_LIST["zipped_file_name"]
        self.testfile_name = ZIP_TEST_FILENAME_LIST["testfile_name"]

        # paths used in making outside.zip
        dir_path1 = os.path.join(self.tmpdir.name, self.dir_name1)
        dir_path2 = os.path.join(self.tmpdir.name, self.dir_name2)
        testfile_path = os.path.join(self.tmpdir.name, self.testfile_name)
        nested_dir_path = os.path.join(self.tmpdir.name, self.nested_dir_name)
        zipped_file_path = os.path.join(dir_path2, self.zipped_file_name)
        nested_zipped_file_path = os.path.join(
            nested_dir_path, self.nested_zipped_file_name)
        nested_zip_file_path = os.path.join(
            dir_path1, self.nested_zip_file_name)

        # paths used in tests
        self.zip_file_path = self.zip_file_name + ".zip"
        self.zipped_file_path = os.path.join(self.dir_name2,
                                             self.zipped_file_name)
        self.nested_zip_path = os.path.join(
            self.dir_name1, self.nested_zip_file_name)
        self.nested_zipped_file_path = os.path.join(
            self.nested_dir_name, self.nested_zipped_file_name)

        os.mkdir(dir_path1)
        os.mkdir(dir_path2)
        os.mkdir(nested_dir_path)

        with open(zipped_file_path, "w") as tmpfile:
            tmpfile.write(self.test_string)

        with open(nested_zipped_file_path, "w") as tmpfile:
            tmpfile.write(self.nested_test_string)

        with open(testfile_path, "w") as tmpfile:
            tmpfile.write(self.test_string)

        make_zip(nested_zip_file_path,
                 root_dir=self.tmpdir.name,
                 base_dir=self.nested_dir_name)
        shutil.rmtree(nested_dir_path)

        # this will include outside.zip itself into the zip
        make_zip(self.zip_file_path,
                 root_dir=self.tmpdir.name,
                 base_dir=".")

    def tearDown(self):
        self.tmpdir.cleanup()
        pfio.remove(self.zip_file_path)

    def test_read_bytes(self):
        with self.fs_handler.open_as_container(
                os.path.abspath(self.zip_file_path)) as handler:
            with handler.open(self.zipped_file_path, "rb") as zipped_file:
                self.assertEqual(self.test_string_b, zipped_file.read())

    def test_read_string(self):
        with self.fs_handler.open_as_container(
                os.path.abspath(self.zip_file_path)) as handler:
            with handler.open(self.zipped_file_path, "r") as zipped_file:
                self.assertEqual(self.test_string, zipped_file.readline())

    @pytest.mark.skipif(sys.version_info < (3, 6),
                        reason="requires python3.6 or higher")
    def test_write_bytes(self):
        testfile_name = "testfile3"
        test_string = "this is a written string\n"
        test_string_b = test_string.encode("utf-8")

        with self.fs_handler.open_as_container(
                os.path.abspath(self.zip_file_path)) as handler:
            with handler.open(testfile_name, "wb") as zipped_file:
                zipped_file.write(test_string_b)

        with self.fs_handler.open_as_container(
                os.path.abspath(self.zip_file_path)) as handler:
            with handler.open(testfile_name, "rb") as zipped_file:
                self.assertEqual(test_string_b, zipped_file.readline())

    @pytest.mark.skipif(sys.version_info < (3, 6),
                        reason="requires python3.6 or higher")
    def test_write_string(self):
        testfile_name = "testfile3"
        test_string = "this is a written string\n"
        with self.fs_handler.open_as_container(
                os.path.abspath(self.zip_file_path)) as handler:
            with handler.open(testfile_name, "w") as zipped_file:
                zipped_file.write(test_string)

        with self.fs_handler.open_as_container(
                os.path.abspath(self.zip_file_path)) as handler:
            with handler.open(testfile_name, "r") as zipped_file:
                self.assertEqual(test_string, zipped_file.readline())

    def test_open_non_exist(self):

        non_exist_file = "non_exist_file.txt"

        with self.fs_handler.open_as_container(non_exist_file) as handler:
            self.assertRaises(IOError, handler.open, non_exist_file)

    @parameterized.expand([
        # not normalized path
        ['././{}//../{}/{}'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                                   ZIP_TEST_FILENAME_LIST["dir_name2"],
                                   ZIP_TEST_FILENAME_LIST["zipped_file_name"])]
    ])
    def test_open_non_normalized_path(self, path_or_prefix):
        with self.fs_handler.open_as_container(
                os.path.abspath(self.zip_file_path)) as handler:
            with handler.open(path_or_prefix, "r") as zipped_file:
                self.assertEqual(self.test_string, zipped_file.read())

    @parameterized.expand([
        # default case get the first level from the root
        ["",
         [ZIP_TEST_FILENAME_LIST["dir_name1"],
          ZIP_TEST_FILENAME_LIST["dir_name2"],
          ZIP_TEST_FILENAME_LIST["testfile_name"]],
         False],
        # Problem 1 in issue #66
        [ZIP_TEST_FILENAME_LIST["dir_name2"],
         [ZIP_TEST_FILENAME_LIST["zipped_file_name"]],
         False],
        # problem 2 in issue #66
        [ZIP_TEST_FILENAME_LIST["dir_name2"],
         [ZIP_TEST_FILENAME_LIST["zipped_file_name"]],
         False],
        # not normalized path
        ['{}//{}//../'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                              ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
         [ZIP_TEST_FILENAME_LIST["zipped_file_name"]],
         False],
        # not normalized path root
        ['{}//..//'.format(ZIP_TEST_FILENAME_LIST["dir_name2"]),
         [ZIP_TEST_FILENAME_LIST["dir_name1"],
          ZIP_TEST_FILENAME_LIST["dir_name2"],
          ZIP_TEST_FILENAME_LIST["testfile_name"]],
         False],
        # not normalized path beyond root
        ['//..//',
         [ZIP_TEST_FILENAME_LIST["dir_name1"],
          ZIP_TEST_FILENAME_LIST["dir_name2"],
          ZIP_TEST_FILENAME_LIST["testfile_name"]],
         False],
        # not normalized path beyond root
        ['{}//..//'.format(ZIP_TEST_FILENAME_LIST["dir_name2"]),
         [ZIP_TEST_FILENAME_LIST["dir_name1"],
          ZIP_TEST_FILENAME_LIST["dir_name2"],
          ZIP_TEST_FILENAME_LIST["testfile_name"]],
         False],
        # starting with slash
        ['/',
         [ZIP_TEST_FILENAME_LIST["dir_name1"],
          ZIP_TEST_FILENAME_LIST["dir_name2"],
          ZIP_TEST_FILENAME_LIST["testfile_name"]],
         False],
        # recursive test
        ['',
         [ZIP_TEST_FILENAME_LIST["dir_name1"],
          ZIP_TEST_FILENAME_LIST["dir_name2"],
          os.path.join(ZIP_TEST_FILENAME_LIST["dir_name1"],
                       ZIP_TEST_FILENAME_LIST["nested_zip_file_name"]),
          os.path.join(ZIP_TEST_FILENAME_LIST["dir_name2"],
                       ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
          ZIP_TEST_FILENAME_LIST["testfile_name"]],
         True],
        [ZIP_TEST_FILENAME_LIST["dir_name2"],
         [ZIP_TEST_FILENAME_LIST["zipped_file_name"]],
         True],
        # problem 2 in issue #66
        [ZIP_TEST_FILENAME_LIST["dir_name2"],
         [ZIP_TEST_FILENAME_LIST["zipped_file_name"]],
         True],
        # not normalized path
        ['{}//{}//../'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                              ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
         [ZIP_TEST_FILENAME_LIST["zipped_file_name"]],
         True],
        # not normalized path root
        ['{}//..//'.format(ZIP_TEST_FILENAME_LIST["dir_name2"]),
         [ZIP_TEST_FILENAME_LIST["dir_name1"],
          ZIP_TEST_FILENAME_LIST["dir_name2"],
          os.path.join(ZIP_TEST_FILENAME_LIST["dir_name1"],
                       ZIP_TEST_FILENAME_LIST["nested_zip_file_name"]),
          os.path.join(ZIP_TEST_FILENAME_LIST["dir_name2"],
                       ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
          ZIP_TEST_FILENAME_LIST["testfile_name"]],
         True],
        # not normalized path beyond root
        ['//..//',
         [ZIP_TEST_FILENAME_LIST["dir_name1"],
          ZIP_TEST_FILENAME_LIST["dir_name2"],
          os.path.join(ZIP_TEST_FILENAME_LIST["dir_name1"],
                       ZIP_TEST_FILENAME_LIST["nested_zip_file_name"]),
          os.path.join(ZIP_TEST_FILENAME_LIST["dir_name2"],
                       ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
          ZIP_TEST_FILENAME_LIST["testfile_name"]],
         True],
        # starting with slash
        ['/',
         [ZIP_TEST_FILENAME_LIST["dir_name1"],
          ZIP_TEST_FILENAME_LIST["dir_name2"],
          os.path.join(ZIP_TEST_FILENAME_LIST["dir_name1"],
                       ZIP_TEST_FILENAME_LIST["nested_zip_file_name"]),
          os.path.join(ZIP_TEST_FILENAME_LIST["dir_name2"],
                       ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
          ZIP_TEST_FILENAME_LIST["testfile_name"]],
         True]]
    )
    def test_list(self, path_or_prefix, expected_list, recursive):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            zip_generator = handler.list(path_or_prefix,
                                         recursive=recursive)
            zip_list = list(zip_generator)
            self.assertEqual(sorted(expected_list),
                             sorted(zip_list))

    @parameterized.expand([
        # non_exist_file
        ['does_not_exist', FileNotFoundError],
        # not exist but share the prefix
        ['{}'.format(ZIP_TEST_FILENAME_LIST["dir_name2"][:1]),
            FileNotFoundError],
        # broken path
        ['{}//{}/'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                          ZIP_TEST_FILENAME_LIST["zipped_file_name"][:1]),
         FileNotFoundError],
        # list a file
        ['{}//{}///'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                            ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
         NotADirectoryError]
    ])
    def test_list_with_errors(self, path_or_prefix, error):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            with self.assertRaises(error):
                list(handler.list(path_or_prefix))

            with self.assertRaises(error):
                list(handler.list(path_or_prefix, recursive=True))

    def test_info(self):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertIsInstance(handler.info(), str)

    @parameterized.expand([
        # path ends with slash
        ['{}//'.format(ZIP_TEST_FILENAME_LIST["dir_name2"]),
         True],
        # not normalized path
        ['{}//{}'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                         ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
         False],
        ['{}//..//{}/{}'.format(ZIP_TEST_FILENAME_LIST["dir_name1"],
                                ZIP_TEST_FILENAME_LIST["dir_name2"],
                                ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
         False],
        # problem 2 in issue #66
        [ZIP_TEST_FILENAME_LIST["dir_name2"],
         True],
        # not normalized path
        ['{}//{}//../'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                              ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
         True],
        # not normalized path root
        ['{}//..//'.format(ZIP_TEST_FILENAME_LIST["dir_name2"]),
         False],
        # not normalized path beyond root
        ['//..//',
         False],
        # starting with slash
        ['/',
         False]]
    )
    def test_isdir(self, path_or_prefix, expected):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertEqual(handler.isdir(path_or_prefix),
                             expected)

    @parameterized.expand(NON_EXIST_LIST)
    def test_isdir_non_exist(self, path_or_prefix):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertFalse(handler.isdir(path_or_prefix))

    def test_mkdir(self):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertRaises(io.UnsupportedOperation, handler.mkdir, "test")

    def test_makedirs(self):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertRaises(io.UnsupportedOperation,
                              handler.makedirs, "test/test")

    def test_pickle(self):
        pickle_file_name = "test_pickle.pickle"
        test_data = {'test_elem1': b'balabala',
                     'test_elem2': 'balabala'}
        pickle_zip = "test_pickle.zip"

        with open(pickle_file_name, "wb") as f:
            pickle.dump(test_data, f)

        with ZipFile(pickle_zip, "w") as test_zip:
            test_zip.write(pickle_file_name)

        with self.fs_handler.open_as_container(pickle_zip) as handler:
            with handler.open(pickle_file_name, 'rb') as f:
                loaded_obj = pickle.load(f)
                self.assertEqual(test_data, loaded_obj)

        os.remove(pickle_file_name)
        os.remove(pickle_zip)

    @parameterized.expand([
        # path ends with slash
        ['{}//'.format(ZIP_TEST_FILENAME_LIST["dir_name2"]),
         True],
        # not normalized path
        ['{}//{}'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                         ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
         True],
        ['{}//..//{}/{}'.format(ZIP_TEST_FILENAME_LIST["dir_name1"],
                                ZIP_TEST_FILENAME_LIST["dir_name2"],
                                ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
         True],
        ['{}//..//{}/{}'.format(ZIP_TEST_FILENAME_LIST["dir_name1"],
                                ZIP_TEST_FILENAME_LIST["dir_name2"],
                                ZIP_TEST_FILENAME_LIST["zipped_file_name"][:-1]
                                ),
         False],
        # # not normalized path
        ['{}//{}//../'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                              ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
         True],
        # not normalized path root
        ['{}//..//'.format(ZIP_TEST_FILENAME_LIST["dir_name2"]),
         False],
        # not normalized path beyond root
        ['//..//',
         False],
        # starting with slash
        ['/',
         False]]
    )
    def test_exists(self, path_or_prefix, expected):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertEqual(handler.exists(path_or_prefix),
                             expected)

    @parameterized.expand(NON_EXIST_LIST)
    def test_not_exists(self, non_exist_file):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertFalse(handler.exists(non_exist_file))

    def test_remove(self):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertRaises(io.UnsupportedOperation,
                              handler.remove, "test/test", False)

    def test_nested_zip(self):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            with handler.open_as_container(
                    self.nested_zip_path) as nested_zip:
                with nested_zip.open(self.nested_zipped_file_path) as f:
                    self.assertEqual(f.read(), self.nested_test_string_b)

                with nested_zip.open(self.nested_zipped_file_path, "r") as f:
                    self.assertEqual(f.read(), self.nested_test_string)

    @parameterized.expand([
        # path ends with slash
        ['{}//'.format(ZIP_TEST_FILENAME_LIST["dir_name2"]),
         '{}/'.format(ZIP_TEST_FILENAME_LIST["dir_name2"])],
        # not normalized path
        ['{}//{}'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                         ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
         '{}/{}'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                        ZIP_TEST_FILENAME_LIST["zipped_file_name"])],
        ['{}//..//{}/{}'.format(ZIP_TEST_FILENAME_LIST["dir_name1"],
                                ZIP_TEST_FILENAME_LIST["dir_name2"],
                                ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
         '{}/{}'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                        ZIP_TEST_FILENAME_LIST["zipped_file_name"])],
        ['{}//{}//../'.format(ZIP_TEST_FILENAME_LIST["dir_name2"],
                              ZIP_TEST_FILENAME_LIST["zipped_file_name"]),
            '{}/'.format(ZIP_TEST_FILENAME_LIST["dir_name2"])]
    ])
    def test_stat(self, path_or_prefix, expected):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertEqual(expected, handler.stat(path_or_prefix).filename)

    @parameterized.expand([
        # not normalized path root
        '{}//..//'.format(ZIP_TEST_FILENAME_LIST["dir_name2"]),
        # not normalized path beyond root
        '//..//',
        # root
        '/'] + NON_EXIST_LIST)
    def test_stat_non_exist(self, path_or_prefix):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            with self.assertRaises(FileNotFoundError):
                handler.stat(path_or_prefix)

    def test_stat_file(self):
        test_file_name = 'testdir2/testfile1'
        expected = ZipFile(self.zip_file_path).getinfo(test_file_name)

        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            stat = handler.stat(test_file_name)
            self.assertIsInstance(stat, pfio.containers.zip.ZipFileStat)
            self.assertTrue(stat.filename.endswith(test_file_name))
            self.assertEqual(stat.size, expected.file_size)
            self.assertEqual(stat.mode, expected.external_attr >> 16)
            self.assertFalse(stat.isdir())

            expected_mtime = datetime(*expected.date_time).timestamp()
            self.assertIsInstance(stat.last_modified, float)
            self.assertEqual(stat.last_modified, expected_mtime)

            for k in ('filename', 'orig_filename', 'comment', 'create_system',
                      'create_version', 'extract_version', 'flag_bits',
                      'volume', 'internal_attr', 'external_attr', 'CRC',
                      'header_offset', 'compress_size', 'compress_type'):
                self.assertEqual(getattr(stat, k), getattr(expected, k))

    def test_stat_directory(self):
        test_dir_name = 'testdir2/'
        expected = ZipFile(self.zip_file_path).getinfo(test_dir_name)

        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            stat = handler.stat(test_dir_name)
            self.assertIsInstance(stat, pfio.containers.zip.ZipFileStat)
            self.assertTrue(stat.filename.endswith(test_dir_name))
            self.assertEqual(stat.size, expected.file_size)
            self.assertEqual(stat.mode, expected.external_attr >> 16)
            self.assertTrue(stat.isdir())

            expected_mtime = datetime(*expected.date_time).timestamp()
            self.assertIsInstance(stat.last_modified, float)
            self.assertEqual(stat.last_modified, expected_mtime)

            for k in ('filename', 'orig_filename', 'comment', 'create_system',
                      'create_version', 'extract_version', 'flag_bits',
                      'volume', 'internal_attr', 'external_attr', 'CRC',
                      'header_offset', 'compress_size', 'compress_type'):
                self.assertEqual(getattr(stat, k), getattr(expected, k))

    @pytest.mark.skipif(sys.version_info < (3, 6),
                        reason="requires python3.6 or higher")
    def test_writing_after_listing(self):
        testfile_name = "testfile3"
        test_string = "this is a written string\n"

        with self.fs_handler.open_as_container(
                os.path.abspath(self.zip_file_path)) as handler:
            list(handler.list())
            self.assertEqual(handler.zip_file_obj_mode, "r")

            with handler.open(testfile_name, "w") as zipped_file:
                zipped_file.write(test_string)
            self.assertEqual(handler.zip_file_obj_mode, "w")

    @pytest.mark.skipif(sys.version_info > (3, 5),
                        reason="requires python3.5 or lower")
    def test_mode_w_exception(self):
        testfile_name = "testfile3"
        test_string = "this is a written string\n"

        with self.fs_handler.open_as_container(
                os.path.abspath(self.zip_file_path)) as handler:
            with self.assertRaises(ValueError):
                with handler.open(testfile_name, "w") as zipped_file:
                    zipped_file.write(test_string)


class TestZipHandlerWithLargeData(unittest.TestCase):
    def setUp(self):
        # The following zip layout is created for all the tests
        # outside.zip
        # | - testfile1

        n = 1 << 20
        self.test_string = make_random_str(n)
        self.fs_handler = pfio.create_handler("posix")

        # the most outside zip
        self.zip_file_name = "outside"

        # nested zip and nested file
        self.tmpdir = tempfile.TemporaryDirectory()

        # test file
        self.testfile_name = "testfile1"

        # paths used in making outside.zip
        testfile_path = os.path.join(self.tmpdir.name, self.testfile_name)

        # paths used in tests
        self.zip_file_path = self.zip_file_name + ".zip"

        with open(testfile_path, "w") as tmpfile:
            tmpfile.write(self.test_string)

        # this will include outside.zip itself into the zip
        make_zip(self.zip_file_path,
                 root_dir=self.tmpdir.name,
                 base_dir=".")

    def tearDown(self):
        self.tmpdir.cleanup()
        pfio.remove(self.zip_file_path)

    def test_read_multi_processes(self):
        barrier = multiprocessing.Barrier(2)
        with self.fs_handler.open_as_container(
                os.path.abspath(self.zip_file_path)) as handler:
            with handler.open(self.testfile_name) as f:
                f.read()

            def func():
                # accessing the shared container
                with handler.open(self.testfile_name) as f:
                    barrier.wait()
                    f.read()

            p1 = multiprocessing.Process(target=func)
            p2 = multiprocessing.Process(target=func)
            p1.start()
            p2.start()

            p1.join(timeout=1)
            p2.join(timeout=1)

            self.assertEqual(p1.exitcode, 0)
            self.assertEqual(p2.exitcode, 0)


NO_DIRECTORY_FILENAME_LIST = {
    "dir1_name": "testdir1",
    "dir2_name": "testdir2",
    "dir3_name": "testdir3",
    "testfile1_name": "testfile1",
    "testfile2_name": "testfile2",
    "testfile3_name": "testfile3",
    "testfile4_name": "testfile4",
}


class TestZipHandlerListNoDirectory(unittest.TestCase):
    def setUp(self):
        # The following zip layout is created for all the tests
        # The difference is despite showing in the following layout for
        # readabilty, the directories are not included in the zip
        # outside.zip
        # | - testdir1
        # | - | - testfile1
        # | - | - testdir2
        # | - | - | - testfile2
        # | - testdir3
        # |   | - testfile3
        # | - testfile4

        self.test_string = "this is a test string\n"
        self.fs_handler = pfio.create_handler("posix")

        # the most outside zip
        self.zip_file_name = "outside.zip"

        # nested zip and nested file
        self.tmpdir = tempfile.TemporaryDirectory()

        # directory and file
        self.dir1_name = NO_DIRECTORY_FILENAME_LIST["dir1_name"]
        self.dir2_name = NO_DIRECTORY_FILENAME_LIST["dir2_name"]
        self.dir3_name = NO_DIRECTORY_FILENAME_LIST["dir3_name"]
        self.testfile1_name = NO_DIRECTORY_FILENAME_LIST["testfile1_name"]
        self.testfile2_name = NO_DIRECTORY_FILENAME_LIST["testfile2_name"]
        self.testfile3_name = NO_DIRECTORY_FILENAME_LIST["testfile3_name"]
        self.testfile4_name = NO_DIRECTORY_FILENAME_LIST["testfile4_name"]

        # paths used in making outside.zip
        dir1_path = os.path.join(self.tmpdir.name, self.dir1_name)
        dir2_path = os.path.join(dir1_path, self.dir2_name)
        dir3_path = os.path.join(self.tmpdir.name, self.dir3_name)
        testfile1_path = os.path.join(dir1_path, self.testfile1_name)
        testfile2_path = os.path.join(dir2_path, self.testfile2_name)
        testfile3_path = os.path.join(dir3_path, self.testfile3_name)
        testfile4_path = os.path.join(self.tmpdir.name, self.testfile4_name)

        # paths used in tests
        for dir in [dir1_path, dir2_path, dir3_path]:
            os.mkdir(dir)

        for file_path in [testfile1_path, testfile2_path,
                          testfile3_path, testfile4_path]:
            with open(file_path, "w") as f:
                f.write(self.test_string)

        # create zip without directory
        self.pwd = os.getcwd()
        os.chdir(self.tmpdir.name)
        cmd = ["zip", "-rD", self.zip_file_name, "."]

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        assert stderr == b""

    def tearDown(self):
        os.chdir(self.pwd)
        self.tmpdir.cleanup()

    @parameterized.expand([
        # default case get the first level from the root
        ["", [NO_DIRECTORY_FILENAME_LIST["dir1_name"],
              NO_DIRECTORY_FILENAME_LIST["dir3_name"],
              NO_DIRECTORY_FILENAME_LIST["testfile4_name"]],
         False],
        # Problem 1 in issue #66
        [NO_DIRECTORY_FILENAME_LIST["dir1_name"],
         [NO_DIRECTORY_FILENAME_LIST["testfile1_name"],
          NO_DIRECTORY_FILENAME_LIST["dir2_name"]],
         False],
        # problem 2 in issue #66
        [os.path.join(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                      NO_DIRECTORY_FILENAME_LIST["dir2_name"]),
         [NO_DIRECTORY_FILENAME_LIST["testfile2_name"]],
         False],
        # not normalized path
        ['{}//{}//../'.format(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                              NO_DIRECTORY_FILENAME_LIST["testfile1_name"]),
         [NO_DIRECTORY_FILENAME_LIST["testfile1_name"],
          NO_DIRECTORY_FILENAME_LIST["dir2_name"]],
         False],
        # not normalized path root
        ['{}//..//'.format(NO_DIRECTORY_FILENAME_LIST["dir1_name"]),
         [NO_DIRECTORY_FILENAME_LIST["dir1_name"],
          NO_DIRECTORY_FILENAME_LIST["dir3_name"],
          NO_DIRECTORY_FILENAME_LIST["testfile4_name"]],
         False],
        # not normalized path beyond root
        ['//..//',
         [NO_DIRECTORY_FILENAME_LIST["dir1_name"],
          NO_DIRECTORY_FILENAME_LIST["dir3_name"],
          NO_DIRECTORY_FILENAME_LIST["testfile4_name"]],
         False],
        # not normalized path beyond root
        ['{}//..//'.format(NO_DIRECTORY_FILENAME_LIST["dir1_name"]),
         [NO_DIRECTORY_FILENAME_LIST["dir1_name"],
          NO_DIRECTORY_FILENAME_LIST["dir3_name"],
          NO_DIRECTORY_FILENAME_LIST["testfile4_name"]],
         False],
        # starting with slash
        ['/', [NO_DIRECTORY_FILENAME_LIST["dir1_name"],
               NO_DIRECTORY_FILENAME_LIST["dir3_name"],
               NO_DIRECTORY_FILENAME_LIST["testfile4_name"]],
         False],
        # recursive test
        ['',
         [os.path.join(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile1_name"]),
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                       NO_DIRECTORY_FILENAME_LIST["dir2_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile2_name"]),
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir3_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile3_name"]),
          NO_DIRECTORY_FILENAME_LIST["testfile4_name"]],
         True],
        [NO_DIRECTORY_FILENAME_LIST["dir1_name"],
         [NO_DIRECTORY_FILENAME_LIST["testfile1_name"],
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir2_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile2_name"])],
         True],
        # problem 2 in issue #66
        [NO_DIRECTORY_FILENAME_LIST["dir1_name"],
         [NO_DIRECTORY_FILENAME_LIST["testfile1_name"],
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir2_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile2_name"])],
         True],
        # not normalized path
        ['{}//{}//../'.format(
            NO_DIRECTORY_FILENAME_LIST["dir1_name"],
            NO_DIRECTORY_FILENAME_LIST["testfile1_name"]),
         [NO_DIRECTORY_FILENAME_LIST["testfile1_name"],
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir2_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile2_name"])],
         True],
        # not normalized path root
        ['{}//..//'.format(NO_DIRECTORY_FILENAME_LIST["dir2_name"]),
         [os.path.join(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile1_name"]),
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                       NO_DIRECTORY_FILENAME_LIST["dir2_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile2_name"]),
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir3_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile3_name"]),
          NO_DIRECTORY_FILENAME_LIST["testfile4_name"]],
         True],
        # not normalized path beyond root
        ['//..//',
         [os.path.join(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile1_name"]),
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                       NO_DIRECTORY_FILENAME_LIST["dir2_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile2_name"]),
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir3_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile3_name"]),
          NO_DIRECTORY_FILENAME_LIST["testfile4_name"]],
         True],
        # not normalized path beyond root
        ['{}//..//../'.format(NO_DIRECTORY_FILENAME_LIST["dir2_name"]),
         [os.path.join(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile1_name"]),
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                       NO_DIRECTORY_FILENAME_LIST["dir2_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile2_name"]),
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir3_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile3_name"]),
          NO_DIRECTORY_FILENAME_LIST["testfile4_name"]],
         True],
        # starting with slash
        ['/',
         [os.path.join(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile1_name"]),
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                       NO_DIRECTORY_FILENAME_LIST["dir2_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile2_name"]),
          os.path.join(NO_DIRECTORY_FILENAME_LIST["dir3_name"],
                       NO_DIRECTORY_FILENAME_LIST["testfile3_name"]),
          NO_DIRECTORY_FILENAME_LIST["testfile4_name"]],
         True]
    ])
    def test_list(self, path_or_prefix, expected_list, recursive):
        with self.fs_handler.open_as_container(self.zip_file_name) as handler:
            zip_generator = handler.list(path_or_prefix,
                                         recursive=recursive)
            zip_list = list(zip_generator)
            self.assertEqual(sorted(expected_list),
                             sorted(zip_list))

    @parameterized.expand([
        # non_exist_file
        ['does_not_exist', FileNotFoundError],
        # not exist but share the prefix
        ['t', FileNotFoundError],
        # broken path
        ['{}//t/'.format(NO_DIRECTORY_FILENAME_LIST["dir1_name"]),
            FileNotFoundError],
        # list a file
        ['{}//{}///'.format(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                            NO_DIRECTORY_FILENAME_LIST["testfile1_name"]),
         NotADirectoryError],
        # list a non_exist_dir but share the surfix
        ['{}/'.format(NO_DIRECTORY_FILENAME_LIST["dir1_name"][:-1]),
         FileNotFoundError]
    ])
    def test_list_with_errors(self, path_or_prefix, error):
        with self.fs_handler.open_as_container(self.zip_file_name) as handler:
            with self.assertRaises(error):
                list(handler.list(path_or_prefix))

            with self.assertRaises(error):
                list(handler.list(path_or_prefix, recursive=True))

    @parameterized.expand([
        # path ends with slash
        ['{}//'.format(NO_DIRECTORY_FILENAME_LIST["dir1_name"]), True],
        # not normalized path
        ['{}//{}'.format(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                         NO_DIRECTORY_FILENAME_LIST["testfile1_name"]),
         False],
        ['{}//..//{}/{}'.format(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                                NO_DIRECTORY_FILENAME_LIST["dir2_name"],
                                NO_DIRECTORY_FILENAME_LIST["testfile1_name"]),
         False],
        # problem 2 in issue #66
        [NO_DIRECTORY_FILENAME_LIST["dir1_name"], True],
        # not normalized path
        ['{}//{}//../'.format(NO_DIRECTORY_FILENAME_LIST["dir1_name"],
                              NO_DIRECTORY_FILENAME_LIST["testfile1_name"]),
         True],
        # not normalized path root
        ['{}//..//'.format(NO_DIRECTORY_FILENAME_LIST["dir1_name"]), False],
        # not normalized path beyond root
        ['//..//', False],
        # not normalized path beyond root
        ['{}//..//'.format(NO_DIRECTORY_FILENAME_LIST["dir1_name"]), False],
        # starting with slash
        ['/', False]
    ])
    def test_isdir(self, path_or_prefix, expected):
        with self.fs_handler.open_as_container(self.zip_file_name) as handler:
            self.assertEqual(handler.isdir(path_or_prefix),
                             expected)

    @parameterized.expand([
        ["does_not_exist"],
        ["does_not_exist/"],
        ["does/not/exist"]
    ])
    def test_isdir_not_exist(self, dir):
        with self.fs_handler.open_as_container(self.zip_file_name) as handler:
            self.assertFalse(handler.isdir(dir))
