import unittest

import chainerio
import io
import os
import pickle
import shutil
import tempfile
from zipfile import ZipFile


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
        self.fs_handler = chainerio.create_handler("posix")

        # the most outside zip
        self.zip_file_name = "outside"

        # nested zip and nested file
        self.tmpdir = tempfile.TemporaryDirectory()
        self.nested_zipped_file_name = "nested"
        self.nested_dir_name = "nested_dir/"
        self.nested_dir_path = os.path.join(self.tmpdir.name,
                                            self.nested_dir_name)
        self.nested_zip_file_name = "nested.zip"

        # directory and file
        self.dir_name1 = "testdir1/"
        self.dir_name2 = "testdir2/"
        self.zipped_file_name = "testfile1"
        self.testfile_name = "testfile2"

        # paths used in making outside.zip
        dir_path1 = os.path.join(self.tmpdir.name, self.dir_name1)
        dir_path2 = os.path.join(self.tmpdir.name, self.dir_name2)
        testfile_path = os.path.join(self.tmpdir.name, self.testfile_name)
        nested_dir_path = os.path.join(self.tmpdir.name, self.nested_dir_name)
        zipped_file_path = os.path.join(dir_path2, self.zipped_file_name)
        nested_zipped_file_path = os.path.join(
            nested_dir_path, self.nested_zipped_file_name)
        nested_zip_file_path = os.path.join(
            dir_path1, self.nested_zipped_file_name)

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

        shutil.make_archive(nested_zip_file_path, "zip",
                            root_dir=self.tmpdir.name,
                            base_dir=self.nested_dir_name)
        shutil.rmtree(nested_dir_path)

        # this will include outside.zip itself into the zip
        shutil.make_archive(self.zip_file_name, "zip",
                            root_dir=self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()
        chainerio.remove(self.zip_file_path)

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

    def test_open_non_exist(self):

        non_exist_file = "non_exist_file.txt"

        with self.fs_handler.open_as_container(non_exist_file) as handler:
            self.assertRaises(IOError, handler.open, non_exist_file)

    def test_list(self):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            zip_generator = handler.list()
            # list the first level of files:
            # expected_list = [testdir1, testdir2, testfile2]
            expected_list = [self.dir_name1.rstrip('/'),
                             self.dir_name2.rstrip('/'),
                             self.testfile_name]
            zip_list = list(zip_generator)
            self.assertEqual(sorted(expected_list), sorted(zip_list))

            # Problem 1, 2 in issue #66
            dir_list = [self.dir_name2, self.dir_name2.rstrip('/')]
            for _dir in dir_list:
                zip_generator = handler.list(_dir)
                zip_list = list(zip_generator)
                # list the testdir2
                # expected_list = [testfile]
                expected_list = [self.zipped_file_name]
                self.assertEqual(sorted(expected_list), sorted(zip_list))

            zip_generator = handler.list(recursive=True)
            # list every thing
            # expected_list = [testdir1, testdir2,
            #                  testdir1/nested.zip, testdir2/testfile,
            #                  testfile2]
            expected_list = [self.dir_name1,
                             self.dir_name2,
                             os.path.join(self.dir_name1,
                                          self.nested_zip_file_name),
                             os.path.join(self.dir_name2,
                                          self.zipped_file_name),
                             self.testfile_name]
            zip_list = list(zip_generator)
            self.assertEqual(sorted(expected_list), sorted(zip_list))

    def test_info(self):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertIsInstance(handler.info(), str)

    def test_isdir(self):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertTrue(handler.isdir(self.dir_name1))
            self.assertFalse(handler.isdir(self.zipped_file_path))

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

    def test_exists(self):
        non_exist_file = "non_exist_file.txt"

        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertTrue(handler.exists(self.dir_name1))
            self.assertTrue(handler.exists(self.zipped_file_path))
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

    def test_stat(self):
        # pass for now
        # TODO(tianqi) add test after we well defined the stat
        pass
