import unittest

from collections.abc import Iterable
from chainerio.filesystems.hdfs import _parse_klist_output
import pickle
import shutil
import os
import getpass

import chainerio


@unittest.skipIf(shutil.which('hdfs') is None, "HDFS client not installed")
class TestHdfsHandler(unittest.TestCase):

    def setUp(self):
        self.test_string = "this is a test string\n"
        self.test_string_b = self.test_string.encode("utf-8")
        self.fs = "hdfs"
        self.tmpfile_name = "tmpfile.txt"

    def test_read_bytes(self):

        with chainerio.create_handler(self.fs) as handler:
            with handler.open(self.tmpfile_name, "wb") as tmpfile:
                tmpfile.write(self.test_string_b)
            with handler.open(self.tmpfile_name, "rb") as f:
                self.assertEqual(self.test_string_b, f.read())

    def test_read_string(self):

        with chainerio.create_handler(self.fs) as handler:
            with handler.open(self.tmpfile_name, "w") as tmpfile:
                tmpfile.write(self.test_string)
            with handler.open(self.tmpfile_name, "r") as f:
                self.assertEqual(self.test_string, f.read())
            with handler.open(self.tmpfile_name, "r") as f:
                self.assertEqual(self.test_string, f.readline())

    def test_read_non_exist(self):

        non_exist_file = "non_exist_file.txt"

        with chainerio.create_handler(self.fs) as handler:
            self.assertRaises(IOError, handler.open, non_exist_file)

    def test_klist_not_exist(self):
        path = os.environ['PATH']
        # remove klist
        os.environ['PATH'] = ''
        with chainerio.create_handler(self.fs) as handler:
            self.assertEqual(getpass.getuser(), handler.username)

        os.environ['PATH'] = path

    def test_keytab_not_exist(self):
        with chainerio.filesystems.hdfs.HdfsFileSystem(
                keytab_path="does_not_exist") as handler:
            self.assertEqual(getpass.getuser(), handler.username)

    def test_principle_pattern(self):
        username = 'fake_user!\"#$%&\'()*+,-./:;<=>?[\\]^ _`{|}~'
        service = 'fake_service!\"#$%&\'()*+,-./:;<=>?[\\]^ _`{|}~'
        correct_out = 'Ticket cache: FILE:/tmp/krb5cc_sdfa\nDefault principal: {}@{}\nValid starting       Expires              Service principal\n10/01/2019 12:44:18  10/08/2019 12:44:14   krbtgt/service@service\nrenew until 10/22/2019 15:04:20'.format(username, service) # NOQA
        self.assertEqual(username,
                         _parse_klist_output(correct_out.encode('utf-8')))

    def test_list(self):
        with chainerio.create_handler(self.fs) as handler:
            file_generator = handler.list()
            self.assertIsInstance(file_generator, Iterable)
            file_list = list(file_generator)
            self.assertIn(self.tmpfile_name, file_list, self.tmpfile_name)

            # An exception is raised when the given path is not a directory
            self.assertRaises(NotADirectoryError, list,
                              handler.list(self.tmpfile_name))
            for test_dir_name in ["testmkdir", "testmkdir/"]:
                nested_dir_name1 = "nested_dir1"
                nested_dir_name2 = "nested_dir2"
                nested_file_name = "file"
                nested_dir1 = os.path.join(test_dir_name, nested_dir_name1)
                nested_dir2 = os.path.join(test_dir_name, nested_dir_name2)
                nested_file = os.path.join(nested_dir2,  nested_file_name)
                nested_file_relative = os.path.join(nested_dir_name2,
                                                    nested_file_name)
                handler.makedirs(nested_dir1)
                handler.makedirs(nested_dir2)

                with handler.open(nested_file, "w") as f:
                    f.write(self.test_string)

                recursive_file_generator = handler.list(test_dir_name,
                                                        recursive=True)
                self.assertIsInstance(recursive_file_generator, Iterable)
                file_list = list(recursive_file_generator)
                self.assertIn(nested_dir_name1, file_list)
                self.assertIn(nested_dir_name2, file_list)
                self.assertIn(nested_file_relative, file_list)

                normal_file_generator = handler.list(test_dir_name)
                self.assertIsInstance(recursive_file_generator, Iterable)
                file_list = list(normal_file_generator)
                self.assertIn(nested_dir_name1, file_list)
                self.assertIn(nested_dir_name2, file_list)
                self.assertNotIn(nested_file_relative, file_list)

                handler.remove(test_dir_name, True)

    def test_info(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertIsInstance(handler.info(), str)

    def test_isdir(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertTrue(handler.isdir("/"))
            self.assertFalse(handler.isdir(self.tmpfile_name))

    def test_mkdir(self):
        test_dir_name = "testmkdir"
        with chainerio.create_handler(self.fs) as handler:
            handler.mkdir(test_dir_name)
            self.assertTrue(handler.isdir(test_dir_name))

            handler.remove(test_dir_name)

    def test_makedirs(self):
        test_dir_name = "testmkdir/"
        nested_dir_name = test_dir_name + "nested_dir"

        with chainerio.create_handler(self.fs) as handler:
            handler.makedirs(nested_dir_name)
            self.assertTrue(handler.isdir(nested_dir_name))

            handler.remove(test_dir_name, True)

    def test_picle(self):

        pickle_file_name = "test_pickle.pickle"
        test_data = {'test_elem1': b'balabala',
                     'test_elem2': 'balabala'}

        with chainerio.create_handler(self.fs) as handler:
            with handler.open(pickle_file_name, 'wb') as f:
                pickle.dump(test_data, f)
            with handler.open(pickle_file_name, 'rb') as f:
                loaded_obj = pickle.load(f)
                self.assertEqual(test_data, loaded_obj)

            handler.remove(pickle_file_name, True)

    def test_exists(self):
        non_exist_file = "non_exist_file.txt"

        with chainerio.create_handler(self.fs) as handler:
            self.assertTrue(handler.exists(self.tmpfile_name))
            self.assertTrue(handler.exists("/"))
            self.assertFalse(handler.exists(non_exist_file))

    def test_rename(self):
        with chainerio.create_handler(self.fs) as handler:
            with handler.open('src', 'w') as fp:
                fp.write('foobar')

            self.assertTrue(handler.exists('src'))
            self.assertFalse(handler.exists('dst'))

            handler.rename('src', 'dst')
            self.assertFalse(handler.exists('src'))
            self.assertTrue(handler.exists('dst'))

            with handler.open('dst', 'r') as fp:
                data = fp.read()
                assert data == 'foobar'

            handler.remove('dst', True)

    def test_remove(self):
        test_file = "test_remove.txt"
        test_dir = "test_dir/"
        nested_dir = os.path.join(test_dir, "nested_file/")
        nested_file = os.path.join(nested_dir, test_file)

        with chainerio.create_handler(self.fs) as handler:
            with handler.open(test_file, 'w') as fp:
                fp.write('foobar')

            # test remove on one file
            self.assertTrue(handler.exists(test_file))
            handler.remove(test_file)
            self.assertFalse(handler.exists(test_file))

            # test remove on directory
            handler.makedirs(nested_dir)
            with handler.open(nested_file, 'w') as fp:
                fp.write('foobar')

            self.assertTrue(handler.exists(test_dir))
            self.assertTrue(handler.exists(nested_dir))
            self.assertTrue(handler.exists(nested_file))

            handler.remove(test_dir, True)

            self.assertFalse(handler.exists(test_dir))
            self.assertFalse(handler.exists(nested_dir))
            self.assertFalse(handler.exists(nested_file))

    def test_stat(self):
        # pass for now
        # TODO(tianqi) add test after we well defined the stat
        pass
