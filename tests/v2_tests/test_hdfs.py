import getpass
import os
import pickle
import shutil
import subprocess
import tempfile
import unittest
from collections.abc import Iterable

from pyarrow import hdfs

from pfio.testing import randstring
from pfio.v2.hdfs import (Hdfs, HdfsFileStat, _get_principal_name_from_keytab,
                          _get_principal_name_from_klist,
                          _parse_principal_name_from_keytab,
                          _parse_principal_name_from_klist)


def create_dummy_keytab(tmpd, dummy_username):
    dummy_password = "123"
    keytab_path = os.path.join(tmpd, "user.keytab")
    command = "(echo 'addent -password -p {}@{} -k 1 -e rc4-hmac' &&\
              sleep 1 && echo {} && echo write_kt {}) \
              | ktutil".format(dummy_username, "dummy_realm",
                               dummy_password, keytab_path)
    pipe = subprocess.Popen(command, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=True)
    out, err = pipe.communicate()
    return keytab_path


@unittest.skipIf(shutil.which('hdfs') is None, "HDFS client not installed")
class TestHdfs(unittest.TestCase):

    def setUp(self):
        self.dirname = randstring()
        self.hdfs = Hdfs()
        self.hdfs.mkdir(self.dirname)

    def tearDown(self):
        self.hdfs.remove(self.dirname, recursive=True)
        self.hdfs.close()

    def test_read_non_exist(self):
        non_exist_file = "non_exist_file.txt"

        with Hdfs() as fs:
            self.assertRaises(IOError, fs.open, non_exist_file)

    @unittest.skipIf(shutil.which('klist') is None, "klist not installed")
    def test_get_principal_name(self):
        original_krb5_ktname = os.environ.get('KRB5_KTNAME')

        with Hdfs() as fs:
            self.assertEqual(getpass.getuser(), fs._get_login_username())

        dummy_username = "IAmADummy"
        with tempfile.TemporaryDirectory() as tmpd:
            try:
                keytab_path = create_dummy_keytab(tmpd, dummy_username)
                os.environ['KRB5_KTNAME'] = keytab_path
                self.assertEqual(dummy_username,
                                 _get_principal_name_from_keytab())

            finally:
                # put KRB5_KTNAME back
                if original_krb5_ktname is None:
                    del os.environ['KRB5_KTNAME']
                else:
                    os.environ['KRB5_KTNAME'] = original_krb5_ktname

    def test_parse_principal_name_from_keytab(self):
        username1 = 'fake_user1!\"#$%&\'()*+,-./:;<=>?[\\]^ _`{|}~'
        username2 = 'fake_user2!\"#$%&\'()*+,-./:;<=>?[\\]^ _`{|}~'
        service = 'fake_service!\"#$%&\'()*+,-./:;<=>?[\\]^ _`{|}~'
        correct_out = 'Keytab name: FILE:user.keytab\n\
                KVNO Principal\n---- ------------------\
                --------------------------------------------------------\n\
                1 {}@{} \n   2 {}@{}\n'.format(username1, service,
                                               username2, service)
        self.assertEqual(username1,
                         (_parse_principal_name_from_keytab(
                             correct_out)))

    def test_parse_principal_name_from_klist(self):
        username = 'fake_user!\"#$%&\'()*+,-./:;<=>?[\\]^ _`{|}~'
        service = 'fake_service!\"#$%&\'()*+,-./:;<=>?[\\]^ _`{|}~'
        correct_out = 'Ticket cache: FILE:/tmp/krb5cc_sdfa\nDefault principal: {}@{}\nValid starting       Expires              Service principal\n10/01/2019 12:44:18  10/08/2019 12:44:14   krbtgt/service@service\nrenew until 10/22/2019 15:04:20'.format(username, service)  # NOQA
        self.assertEqual(username,
                         _parse_principal_name_from_klist(correct_out))

    def test_get_principal_name_from_klist(self):
        assert _get_principal_name_from_klist()

    def test_mkdir(self):
        test_dir_name = "testmkdir"
        with Hdfs(self.dirname) as fs:
            fs.mkdir(test_dir_name)
            self.assertTrue(fs.isdir(test_dir_name))

            fs.remove(test_dir_name)

    def test_makedirs(self):
        test_dir_name = "testmkdir/"
        nested_dir_name = test_dir_name + "nested_dir"

        with Hdfs(self.dirname) as fs:
            fs.makedirs(nested_dir_name)
            self.assertTrue(fs.isdir(nested_dir_name))

            fs.remove(test_dir_name, True)

    def test_pickle(self):
        pickle_file_name = "test_pickle.pickle"
        test_data = {'test_elem1': b'balabala',
                     'test_elem2': 'balabala'}

        with Hdfs(self.dirname) as fs:
            with fs.open(pickle_file_name, 'wb') as f:
                pickle.dump(test_data, f)
            with fs.open(pickle_file_name, 'rb') as f:
                loaded_obj = pickle.load(f)
                self.assertEqual(test_data, loaded_obj)

            fs.remove(pickle_file_name, True)

    def test_rename(self):
        with Hdfs(self.dirname) as fs:
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

            fs.remove('dst', True)

    def test_remove(self):
        test_file = "test_remove.txt"
        test_dir = "test_dir/"
        nested_dir = os.path.join(test_dir, "nested_file/")
        nested_file = os.path.join(nested_dir, test_file)

        with Hdfs(self.dirname) as fs:
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

        with Hdfs(self.dirname) as fs:
            with fs.open(test_file_name, 'w') as fp:
                fp.write('foobar')

            conn = hdfs.connect()
            expected = conn.info(os.path.join(fs.cwd, test_file_name))

            stat = fs.stat(test_file_name)
            self.assertIsInstance(stat, HdfsFileStat)
            self.assertTrue(stat.filename.endswith(test_file_name))
            self.assertFalse(stat.isdir())
            # New PyArrow API doesn't support permission
            # self.assertEqual(stat.mode & 0o777, expected['permissions'])
            self.assertTrue(stat.mode & 0o100000)
            self.assertIsInstance(stat.last_accessed, float)
            self.assertIsInstance(stat.last_modified, float)
            for k in ('size', 'last_accessed', 'last_modified'):
                self.assertEqual(getattr(stat, k), expected[k])

            fs.remove(test_file_name)

    def test_stat_directory(self):
        test_dir_name = "testmkdir"
        with Hdfs(self.dirname) as fs:
            fs.mkdir(test_dir_name)

            stat = fs.stat(test_dir_name)
            self.assertIsInstance(stat, HdfsFileStat)
            self.assertTrue(stat.filename.endswith(test_dir_name))
            self.assertTrue(stat.isdir())

            self.assertTrue(stat.mode & 0o40000)

            fs.remove(test_dir_name)


@unittest.skipIf(shutil.which('hdfs') is None, "HDFS client not installed")
class TestHdfsFsWithFile(unittest.TestCase):

    def setUp(self):
        self.test_string = "this is a test string\n"
        self.tmpfile_name = randstring()

        with Hdfs() as fs:
            with fs.open(self.tmpfile_name, "w") as tmpfile:
                tmpfile.write(self.test_string)

    def tearDown(self):
        with Hdfs() as fs:
            try:
                fs.remove(self.tmpfile_name)
            except IOError:
                pass

    def test_read_string(self):
        with Hdfs() as fs:
            with fs.open(self.tmpfile_name, "r") as f:
                self.assertEqual(self.test_string, f.read())
            with fs.open(self.tmpfile_name, "r") as f:
                self.assertEqual(self.test_string, f.readline())

    def test_list(self):
        with Hdfs() as fs:
            file_generator = fs.list()
            self.assertIsInstance(file_generator, Iterable)
            file_list = list(file_generator)
            self.assertIn(self.tmpfile_name, file_list, self.tmpfile_name)

            # An exception is raised when the given path is not a directory
            self.assertRaises(NotADirectoryError, list,
                              fs.list(self.tmpfile_name))
            for test_dir_name in ["testmkdir", "testmkdir/"]:
                nested_dir_name1 = "nested_dir1"
                nested_dir_name2 = "nested_dir2"
                nested_file_name = "file"
                nested_dir1 = os.path.join(test_dir_name, nested_dir_name1)
                nested_dir2 = os.path.join(test_dir_name, nested_dir_name2)
                nested_file = os.path.join(nested_dir2,  nested_file_name)
                nested_file_relative = os.path.join(nested_dir_name2,
                                                    nested_file_name)

                try:
                    fs.makedirs(nested_dir1)
                    fs.makedirs(nested_dir2)

                    with fs.open(nested_file, "w") as f:
                        f.write(self.test_string)

                    recursive_file_generator = fs.list(test_dir_name,
                                                       recursive=True)
                    self.assertIsInstance(recursive_file_generator, Iterable)
                    file_list = list(recursive_file_generator)
                    self.assertIn(nested_dir_name1, file_list)
                    self.assertIn(nested_dir_name2, file_list)
                    self.assertIn(nested_file_relative, file_list)

                    normal_file_generator = fs.list(test_dir_name)
                    self.assertIsInstance(recursive_file_generator, Iterable)
                    file_list = list(normal_file_generator)
                    self.assertIn(nested_dir_name1, file_list)
                    self.assertIn(nested_dir_name2, file_list)
                    self.assertNotIn(nested_file_relative, file_list)
                finally:
                    fs.remove(test_dir_name, True)

    def test_isdir(self):
        with Hdfs() as fs:
            self.assertTrue(fs.isdir("/"))
            self.assertFalse(fs.isdir(self.tmpfile_name))
            self.assertFalse(fs.isdir("/nonexistent-entity"))

    def test_exists(self):
        non_exist_file = "non_exist_file.txt"

        with Hdfs() as fs:
            self.assertTrue(fs.exists(self.tmpfile_name))
            self.assertTrue(fs.exists("/"))
            self.assertFalse(fs.exists(non_exist_file))


@unittest.skipIf(shutil.which('hdfs') is None, "HDFS client not installed")
class TestHdfsWithBinaryFile(unittest.TestCase):

    def setUp(self):
        test_string = "this is a test string\n"
        self.test_string_b = test_string.encode("utf-8")
        self.fs = "hdfs"
        self.tmpfile_name = "tmpfile.txt"

        with Hdfs() as fs:
            with fs.open(self.tmpfile_name, "wb") as tmpfile:
                tmpfile.write(self.test_string_b)

    def tearDown(self):
        with Hdfs() as fs:
            try:
                fs.remove(self.tmpfile_name)
            except IOError:
                pass

    def test_read_bytes(self):
        with Hdfs() as fs:
            with fs.open(self.tmpfile_name, "rb") as f:
                self.assertEqual(self.test_string_b, f.read())
