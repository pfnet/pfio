import unittest

import chainerio
import io
import multiprocessing
import os
import pickle
import random
import shutil
import string
import tempfile
from zipfile import ZipFile


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
            dir_path1, self.nested_zip_file_name)

        # paths used in tests
        self.zip_file_path = self.zip_file_name + ".zip"
        self.zipped_file_path = os.path.join(self.dir_name2,
                                             self.zipped_file_name)
        self.nested_zip_path = os.path.join(
            self.dir_name1, self.nested_zip_file_name)
        self.nested_zipped_file_path = os.path.join(
            self.nested_dir_name, self.nested_zipped_file_name)

        self.non_exists_list = ["does_not_exist", "does_not_exist/",
                                "does/not/exist"]

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

    def test_open_non_normalized_path(self):
        cases = [
            # not normalized path
            {"path_or_prefix": '././testdir2//../testdir2/testfile1',
             "expected": self.test_string}]
        for case in cases:
            with self.fs_handler.open_as_container(
                    os.path.abspath(self.zip_file_path)) as handler:
                with handler.open(case['path_or_prefix'], "r") as zipped_file:
                    self.assertEqual(case['expected'], zipped_file.read())

    def test_list(self):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            cases = [
                # default case get the first level from the root
                {"path_or_prefix": "",
                 "expected_list": [self.dir_name1.rstrip('/'),
                                   self.dir_name2.rstrip('/'),
                                   self.testfile_name],
                 "recursive": False},
                # Problem 1 in issue #66
                {"path_or_prefix": self.dir_name2,
                 "expected_list": [self.zipped_file_name],
                 "recursive": False},
                # problem 2 in issue #66
                {"path_or_prefix": self.dir_name2.rstrip('/'),
                 "expected_list": [self.zipped_file_name],
                 "recursive": False},
                # not normalized path
                {"path_or_prefix": 'testdir2//testfile//../',
                 "expected_list": [self.zipped_file_name],
                 "recursive": False},
                # not normalized path root
                {"path_or_prefix": 'testdir2//..//',
                 "expected_list": [self.dir_name1.rstrip('/'),
                                   self.dir_name2.rstrip('/'),
                                   self.testfile_name],
                 "recursive": False},
                # not normalized path beyond root
                {"path_or_prefix": '//..//',
                 "expected_list": [self.dir_name1.rstrip('/'),
                                   self.dir_name2.rstrip('/'),
                                   self.testfile_name],
                 "recursive": False},
                # not normalized path beyond root
                {"path_or_prefix": 'testdir2//..//',
                 "expected_list": [self.dir_name1.rstrip('/'),
                                   self.dir_name2.rstrip('/'),
                                   self.testfile_name],
                 "recursive": False},
                # starting with slash
                {"path_or_prefix": '/',
                 "expected_list": [self.dir_name1.rstrip('/'),
                                   self.dir_name2.rstrip('/'),
                                   self.testfile_name],
                 "recursive": False},
                # recursive test
                {"path_or_prefix": '',
                 "expected_list": [self.dir_name1.rstrip('/'),
                                   self.dir_name2.rstrip('/'),
                                   os.path.join(self.dir_name1,
                                                self.nested_zip_file_name),
                                   os.path.join(self.dir_name2,
                                                self.zipped_file_name),
                                   self.testfile_name],
                 "recursive": True},
                {"path_or_prefix": self.dir_name2,
                 "expected_list": [self.zipped_file_name],
                 "recursive": True},
                # problem 2 in issue #66
                {"path_or_prefix": self.dir_name2.rstrip('/'),
                 "expected_list": [self.zipped_file_name],
                 "recursive": True},
                # not normalized path
                {"path_or_prefix": 'testdir2//testfile//../',
                 "expected_list": [self.zipped_file_name],
                 "recursive": True},
                # not normalized path root
                {"path_or_prefix": 'testdir2//..//',
                 "expected_list": [self.dir_name1.rstrip('/'),
                                   self.dir_name2.rstrip('/'),
                                   os.path.join(self.dir_name1,
                                                self.nested_zip_file_name),
                                   os.path.join(self.dir_name2,
                                                self.zipped_file_name),
                                   self.testfile_name],
                 "recursive": True},
                # not normalized path beyond root
                {"path_or_prefix": '//..//',
                 "expected_list": [self.dir_name1.rstrip('/'),
                                   self.dir_name2.rstrip('/'),
                                   os.path.join(self.dir_name1,
                                                self.nested_zip_file_name),
                                   os.path.join(self.dir_name2,
                                                self.zipped_file_name),
                                   self.testfile_name],
                 "recursive": True},
                # not normalized path beyond root
                {"path_or_prefix": 'testdir2//..//',
                 "expected_list": [self.dir_name1.rstrip('/'),
                                   self.dir_name2.rstrip('/'),
                                   os.path.join(self.dir_name1,
                                                self.nested_zip_file_name),
                                   os.path.join(self.dir_name2,
                                                self.zipped_file_name),
                                   self.testfile_name],
                 "recursive": True},
                # starting with slash
                {"path_or_prefix": '/',
                 "expected_list": [self.dir_name1.rstrip('/'),
                                   self.dir_name2.rstrip('/'),
                                   os.path.join(self.dir_name1,
                                                self.nested_zip_file_name),
                                   os.path.join(self.dir_name2,
                                                self.zipped_file_name),
                                   self.testfile_name],
                 "recursive": True}]

            for case in cases:
                zip_generator = handler.list(case['path_or_prefix'],
                                             recursive=case['recursive'])
                zip_list = list(zip_generator)
                self.assertEqual(sorted(case['expected_list']),
                                 sorted(zip_list))

    def test_list_with_errors(self):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            cases = [
                # non_exist_file
                {"path_or_prefix": 'does_not_exist',
                 "error": FileNotFoundError},
                # not exist but share the prefix
                {"path_or_prefix": 't',
                 "error": FileNotFoundError},
                # broken path
                {"path_or_prefix": 'testdir2//t/',
                 "error": FileNotFoundError},
                # list a file
                {"path_or_prefix": 'testdir2//testfile1///',
                 "error": NotADirectoryError}]
            for case in cases:
                with self.assertRaises(case["error"]):
                    list(handler.list(case['path_or_prefix']))

                with self.assertRaises(case["error"]):
                    list(handler.list(case['path_or_prefix'], recursive=True))

    def test_info(self):
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            self.assertIsInstance(handler.info(), str)

    def test_isdir(self):
        cases = [
            # path ends with slash
            {"path_or_prefix": 'testdir2//',
             "expected": True},
            # not normalized path
            {"path_or_prefix": 'testdir2//testfile1',
             "expected": False},
            {"path_or_prefix": 'testdir1//..//testdir2/testfile1',
             "expected": False},
            # problem 2 in issue #66
            {"path_or_prefix": self.dir_name2.rstrip('/'),
             "expected": True},
            # not normalized path
            {"path_or_prefix": 'testdir2//testfile1//../',
             "expected": True},
            # not normalized path root
            {"path_or_prefix": 'testdir2//..//',
             "expected": False},
            # not normalized path beyond root
            {"path_or_prefix": '//..//',
             "expected": False},
            # not normalized path beyond root
            {"path_or_prefix": 'testdir2//..//',
             "expected": False},
            # starting with slash
            {"path_or_prefix": '/',
             "expected": False}]
        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            for case in cases:
                self.assertEqual(handler.isdir(case["path_or_prefix"]),
                                 case["expected"])

            for _dir in self.non_exists_list:
                self.assertFalse(handler.isdir(_dir))

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
        cases = [
            # path ends with slash
            {"path_or_prefix": 'testdir2//',
             "expected": True},
            # not normalized path
            {"path_or_prefix": 'testdir2//testfile1',
             "expected": True},
            {"path_or_prefix": 'testdir1//..//testdir2/testfile1',
             "expected": True},
            {"path_or_prefix": 'testdir1//..//testdir2/testfile',
             "expected": False},
            # # not normalized path
            {"path_or_prefix": 'testdir2//testfile//../',
             "expected": True},
            # not normalized path root
            {"path_or_prefix": 'testdir2//..//',
             "expected": False},
            # not normalized path beyond root
            {"path_or_prefix": '//..//',
             "expected": False},
            # not normalized path beyond root
            {"path_or_prefix": 'testdir2//..//',
             "expected": False},
            # starting with slash
            {"path_or_prefix": '/',
             "expected": False}]

        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            for case in cases:
                self.assertEqual(handler.exists(case["path_or_prefix"]),
                                 case['expected'])

            for case in self.non_exists_list:
                self.assertFalse(handler.exists(case))

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
        cases = [
            # path ends with slash
            {"path_or_prefix": 'testdir2//',
             "expected": 'testdir2/'},
            # not normalized path
            {"path_or_prefix": 'testdir2//testfile1',
             "expected": 'testdir2/testfile1'},
            {"path_or_prefix": 'testdir1//..//testdir2/testfile1',
             "expected": 'testdir2/testfile1'},
            {"path_or_prefix": 'testdir2//testfile//../',
             "expected": 'testdir2/'}]

        non_exists_list = [
            # not normalized path root
            'testdir2//..//',
            # not normalized path beyond root
            '//..//',
            # not normalized path beyond root
            'testdir2//..//',
            # root
            '/'] + self.non_exists_list

        with self.fs_handler.open_as_container(self.zip_file_path) as handler:
            for case in cases:
                self.assertEqual(case['expected'],
                                 handler.stat(case['path_or_prefix']).filename)

            for _dir in non_exists_list:
                with self.assertRaises(FileNotFoundError):
                    handler.stat(_dir)

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


class TestZipHandlerWithLargeData(unittest.TestCase):

    def setUp(self):
        # The following zip layout is created for all the tests
        # outside.zip
        # | - testfile1

        n = 1 << 13
        self.test_string = make_random_str(n)
        self.fs_handler = chainerio.create_handler("posix")

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
        chainerio.remove(self.zip_file_path)

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
