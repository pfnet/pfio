import unittest

from collections.abc import Iterable
from pfio.filesystems.hdfs import _parse_principal_name_from_klist
from pfio.filesystems.hdfs import _parse_principal_name_from_keytab
from pfio.filesystems.hdfs import _get_principal_name_from_klist
from pfio.filesystems.hdfs import HdfsFileSystem
from pfio.filesystems.hdfs import HdfsFileStat
import pickle
import shutil
import subprocess
import os
import getpass
import tempfile

from pyarrow import hdfs

import pfio


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
class TestHdfsHandler(unittest.TestCase):

    def setUp(self):
        self.fs = "hdfs"

    def test_read_non_exist(self):
        non_exist_file = "non_exist_file.txt"

        with pfio.create_handler(self.fs) as handler:
            self.assertRaises(IOError, handler.open, non_exist_file)

    @unittest.skipIf(shutil.which('klist') is None, "klist not installed")
    def test_get_principal_name(self):
        ticket_cache_path = "/tmp/krb5cc_{}".format(os.getuid())
        ticket_cache_backup_path = "/tmp/ccbackup_{}".format(getpass.getuser())

        # remove the credential cache
        if pfio.exists(ticket_cache_path):
            pfio.rename(ticket_cache_path, ticket_cache_backup_path)
        original_ccname = os.environ.get("KRB5CCNAME")
        if original_ccname is not None:
            del os.environ['KRB5CCNAME']

        # remove klist
        original_path = os.environ.get('PATH')
        if original_path is not None:
            del os.environ['PATH']

        # remove keytab
        original_krb5_ktname = os.environ.get('KRB5_KTNAME')
        if original_krb5_ktname is not None:
            del os.environ['KRB5_KTNAME']

        # priority 0:
        # getting login name when klist, cache and keytab are not available
        with HdfsFileSystem() as handler:
            self.assertEqual(getpass.getuser(), handler.username)

        # restore klist
        if original_path is not None:
            os.environ['PATH'] = original_path
        # priority 0:
        # getting login name when cache and keytab are not available
        with HdfsFileSystem() as handler:
            self.assertEqual(getpass.getuser(), handler.username)

        # priority 1:
        # getting keytab username when only cache is not available
        dummy_username = "IAmADummy"
        with tempfile.TemporaryDirectory() as tmpd:
            # save the original KRB5_KTNAME
            try:
                keytab_path = create_dummy_keytab(tmpd, dummy_username)
                os.environ['KRB5_KTNAME'] = keytab_path
                with HdfsFileSystem() as handler:
                    self.assertEqual(dummy_username, handler.username)
            finally:
                # put KRB5_KTNAME back
                if original_krb5_ktname is None:
                    del os.environ['KRB5_KTNAME']
                else:
                    os.environ['KRB5_KTNAME'] = original_krb5_ktname

        # restore cache
        # remove the credential cache
        if pfio.exists(ticket_cache_backup_path):
            pfio.rename(ticket_cache_backup_path, ticket_cache_path)
        if original_ccname is not None:
            os.environ['KRB5CCNAME'] = original_ccname
        # priority 2:
        # getting principal name from cache
        with HdfsFileSystem() as handler:
            self.assertEqual(_get_principal_name_from_klist(),
                             handler.username)

    def test_get_principal_name_from_keytab(self):
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

    def test_get_principal_name_from_klist(self):
        username = 'fake_user!\"#$%&\'()*+,-./:;<=>?[\\]^ _`{|}~'
        service = 'fake_service!\"#$%&\'()*+,-./:;<=>?[\\]^ _`{|}~'
        correct_out = 'Ticket cache: FILE:/tmp/krb5cc_sdfa\nDefault principal: {}@{}\nValid starting       Expires              Service principal\n10/01/2019 12:44:18  10/08/2019 12:44:14   krbtgt/service@service\nrenew until 10/22/2019 15:04:20'.format(username, service)  # NOQA
        self.assertEqual(username,
                         _parse_principal_name_from_klist(correct_out))

    def test_info(self):
        with pfio.create_handler(self.fs) as handler:
            self.assertIsInstance(handler.info(), str)

    def test_mkdir(self):
        test_dir_name = "testmkdir"
        with pfio.create_handler(self.fs) as handler:
            handler.mkdir(test_dir_name)
            self.assertTrue(handler.isdir(test_dir_name))

            handler.remove(test_dir_name)

    def test_makedirs(self):
        test_dir_name = "testmkdir/"
        nested_dir_name = test_dir_name + "nested_dir"

        with pfio.create_handler(self.fs) as handler:
            handler.makedirs(nested_dir_name)
            self.assertTrue(handler.isdir(nested_dir_name))

            handler.remove(test_dir_name, True)

    def test_picle(self):
        pickle_file_name = "test_pickle.pickle"
        test_data = {'test_elem1': b'balabala',
                     'test_elem2': 'balabala'}

        with pfio.create_handler(self.fs) as handler:
            with handler.open(pickle_file_name, 'wb') as f:
                pickle.dump(test_data, f)
            with handler.open(pickle_file_name, 'rb') as f:
                loaded_obj = pickle.load(f)
                self.assertEqual(test_data, loaded_obj)

            handler.remove(pickle_file_name, True)

    def test_rename(self):
        with pfio.create_handler(self.fs) as handler:
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

        with pfio.create_handler(self.fs) as handler:
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

    def test_stat_file(self):
        test_file_name = "testfile"

        with pfio.create_handler(self.fs) as handler:
            with handler.open(test_file_name, 'w') as fp:
                fp.write('foobar')

            conn = hdfs.connect()
            expected = conn.info(test_file_name)

            stat = handler.stat(test_file_name)
            self.assertIsInstance(stat, HdfsFileStat)
            self.assertTrue(stat.filename.endswith(test_file_name))
            self.assertFalse(stat.isdir())
            self.assertEqual(stat.mode & 0o777, expected['permissions'])
            self.assertTrue(stat.mode & 0o100000)
            self.assertIsInstance(stat.last_accessed, float)
            self.assertIsInstance(stat.last_modified, float)
            for k in ('size', 'owner', 'group', 'replication',
                      'block_size', 'kind', 'last_accessed', 'last_modified'):
                self.assertEqual(getattr(stat, k), expected[k])

            handler.remove(test_file_name)

    def test_stat_directory(self):
        test_dir_name = "testmkdir"
        with pfio.create_handler(self.fs) as handler:
            handler.mkdir(test_dir_name)

            conn = hdfs.connect()
            expected = conn.info(test_dir_name)

            stat = handler.stat(test_dir_name)
            self.assertIsInstance(stat, HdfsFileStat)
            self.assertTrue(stat.filename.endswith(test_dir_name))
            self.assertTrue(stat.isdir())
            self.assertEqual(stat.mode & 0o777, expected['permissions'])
            self.assertTrue(stat.mode & 0o40000)
            self.assertIsInstance(stat.last_accessed, float)
            self.assertIsInstance(stat.last_modified, float)
            for k in ('size', 'owner', 'group', 'replication',
                      'block_size', 'kind', 'last_accessed', 'last_modified'):
                self.assertEqual(getattr(stat, k), expected[k])

            handler.remove(test_dir_name)


@unittest.skipIf(shutil.which('hdfs') is None, "HDFS client not installed")
class TestHdfsHandlerWithFile(unittest.TestCase):

    def setUp(self):
        self.test_string = "this is a test string\n"
        self.fs = "hdfs"
        self.tmpfile_name = "tmpfile.txt"

        with pfio.create_handler(self.fs) as handler:
            with handler.open(self.tmpfile_name, "w") as tmpfile:
                tmpfile.write(self.test_string)

    def tearDown(self):
        with pfio.create_handler(self.fs) as handler:
            try:
                handler.remove(self.tmpfile_name)
            except IOError:
                pass

    def test_read_string(self):
        with pfio.create_handler(self.fs) as handler:
            with handler.open(self.tmpfile_name, "r") as f:
                self.assertEqual(self.test_string, f.read())
            with handler.open(self.tmpfile_name, "r") as f:
                self.assertEqual(self.test_string, f.readline())

    def test_list(self):
        with pfio.create_handler(self.fs) as handler:
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

                try:
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
                finally:
                    handler.remove(test_dir_name, True)

    def test_isdir(self):
        with pfio.create_handler(self.fs) as handler:
            self.assertTrue(handler.isdir("/"))
            self.assertFalse(handler.isdir(self.tmpfile_name))

    def test_exists(self):
        non_exist_file = "non_exist_file.txt"

        with pfio.create_handler(self.fs) as handler:
            self.assertTrue(handler.exists(self.tmpfile_name))
            self.assertTrue(handler.exists("/"))
            self.assertFalse(handler.exists(non_exist_file))


@unittest.skipIf(shutil.which('hdfs') is None, "HDFS client not installed")
class TestHdfsHandlerWithBinaryFile(unittest.TestCase):

    def setUp(self):
        test_string = "this is a test string\n"
        self.test_string_b = test_string.encode("utf-8")
        self.fs = "hdfs"
        self.tmpfile_name = "tmpfile.txt"

        with pfio.create_handler(self.fs) as handler:
            with handler.open(self.tmpfile_name, "wb") as tmpfile:
                tmpfile.write(self.test_string_b)

    def tearDown(self):
        with pfio.create_handler(self.fs) as handler:
            try:
                handler.remove(self.tmpfile_name)
            except IOError:
                pass

    def test_read_bytes(self):
        with pfio.create_handler(self.fs) as handler:
            with handler.open(self.tmpfile_name, "rb") as f:
                self.assertEqual(self.test_string_b, f.read())
