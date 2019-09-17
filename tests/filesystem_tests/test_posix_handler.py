import unittest

from collections.abc import Iterable
import os
import pickle
import tempfile


import chainerio


class TestPosixHandler(unittest.TestCase):

    def setUp(self):
        self.test_string_str = "this is a test string\n"
        self.test_string_bytes = self.test_string_str.encode("utf-8")
        self.fs = "posix"

    def test_read_string(self):

        with chainerio.create_handler(self.fs) as handler:
            with tempfile.NamedTemporaryFile("w+t", delete=False) as tmpfile:
                tmpfile_path = tmpfile.name
                tmpfile.write(self.test_string_str)

            with handler.open(tmpfile_path, mode="r") as loaded_file:
                self.assertEqual(self.test_string_str, loaded_file.read())

            with handler.open(tmpfile_path, mode="r") as loaded_file:
                self.assertEqual(self.test_string_str, loaded_file.readline())

            handler.remove(tmpfile_path)

    def test_read_bytes(self):

        with chainerio.create_handler(self.fs) as handler:
            with tempfile.NamedTemporaryFile("w+b", delete=False) as tmpfile:
                tmpfile_path = tmpfile.name
                tmpfile.write(self.test_string_bytes)

            with handler.open(tmpfile_path, mode="rb") as loaded_file:
                self.assertEqual(self.test_string_bytes, loaded_file.read())

            handler.remove(tmpfile_path)

    def test_open_non_exist(self):

        non_exist_file = "non_exist_file.txt"
        if os.path.exists(non_exist_file):
            os.remove(non_exist_file)

        with chainerio.create_handler(self.fs) as handler:
            self.assertRaises(IOError, handler.open, non_exist_file)

    def test_list(self):
        # directory layout
        # testlsdir
        # | - nested_dir1
        # |   | - nested_dir3
        # | _ nested_dir2
        test_dir_name = "testlsdir/"
        nested_dir_name1 = "nested_dir1"
        nested_dir_name2 = "nested_dir2"
        nested_dir_name3 = "nested_dir3"
        nested_dir_path1 = os.path.join(test_dir_name, nested_dir_name1)
        nested_dir_path2 = os.path.join(test_dir_name, nested_dir_name2)
        nested_dir_path3 = os.path.join(nested_dir_path1, nested_dir_name3)
        nested_dir_relative_path3 = os.path.join(nested_dir_name1,
                                                 nested_dir_name3)

        with chainerio.create_handler(self.fs) as handler:
            handler.makedirs(nested_dir_path1)
            handler.makedirs(nested_dir_path2)
            handler.makedirs(nested_dir_path3)

            self.assertIsInstance(handler.list(), Iterable)
            full_list_of_file = list(handler.list(test_dir_name,
                                                  recursive=True))
            self.assertIn(nested_dir_name1, full_list_of_file)
            self.assertIn(nested_dir_name2, full_list_of_file)
            self.assertIn(nested_dir_relative_path3, full_list_of_file)

            first_level_list_of_file = list(handler.list(test_dir_name))
            self.assertIn(nested_dir_name1, first_level_list_of_file)
            self.assertIn(nested_dir_name2, first_level_list_of_file)
            self.assertNotIn(nested_dir_relative_path3,
                             first_level_list_of_file)

            handler.remove(test_dir_name, True)

    def test_info(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertIsInstance(handler.info(), str)

    def test_isdir(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertTrue(handler.isdir("/"))
            self.assertFalse(handler.isdir("test_posix_handler.py"))

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

            handler.remove(pickle_file_name)

    def test_exists(self):
        non_exist_file = "non_exist_file.txt"

        with chainerio.create_handler(self.fs) as handler:
            self.assertTrue(handler.exists(__file__))
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

            handler.remove('dst')

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


if __name__ == '__main__':
    unittest.main()
