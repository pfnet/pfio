import unittest

import chainerio
import os
import shutil


class TestContext(unittest.TestCase):

    def setUp(self):
        self.test_string_str = "this is a test string\n"
        self.test_string_bytes = self.test_string_str.encode("utf-8")
        self.dir_name = "testdir/"
        self.tmpfile_name = "testfile.txt"
        self.tmpfile_path = os.path.join(self.dir_name, self.tmpfile_name)
        os.mkdir(self.dir_name)
        with open(self.tmpfile_path, "w") as tmpfile:
            tmpfile.write(self.test_string_str)

    def tearDown(self):
        chainerio.remove(self.dir_name, True)

    def test_set_root(self):
        # Set default context globally in this process
        chainerio.set_root('posix')

        # Using the context to open local file
        with chainerio.open(self.tmpfile_path, "r") as fp:
            self.assertEqual(fp.read(), self.test_string_str)

        chainerio.set_root('file://' + self.dir_name)
        with chainerio.open(self.tmpfile_name, "r") as fp:
            self.assertEqual(fp.read(), self.test_string_str)

        chainerio.set_root('')

    def test_open_as_container(self):
        # Create a container for testing
        chainerio.set_root("posix")
        zip_file_name = "test"
        zip_file_path = zip_file_name + ".zip"

        shutil.make_archive(zip_file_name, "zip", base_dir=self.dir_name)

        with chainerio.open_as_container(zip_file_path) as container:
            file_generator = container.list()
            file_list = list(file_generator)
            self.assertIn(self.dir_name[:-1], file_list)
            self.assertNotIn(self.tmpfile_path, file_list)
            self.assertNotIn("", file_list)

            file_generator = container.list(self.dir_name)
            file_list = list(file_generator)
            self.assertNotIn(self.dir_name[:-1], file_list)
            self.assertIn(os.path.basename(self.tmpfile_path), file_list)
            self.assertNotIn("", file_list)

            self.assertTrue(container.isdir(self.dir_name))
            self.assertFalse(container.isdir(self.tmpfile_path))

            self.assertIsInstance(container.info(), str)
            with container.open(self.tmpfile_path, "r") as f:
                self.assertEqual(
                    f.read(), self.test_string_str)

        chainerio.remove(zip_file_path)

    def test_fs_detection_on_container_posix(self):
        # Create a container for testing
        zip_file_name = "test"
        zip_file_path = zip_file_name + ".zip"
        posix_file_path = "file://" + zip_file_path

        shutil.make_archive(zip_file_name, "zip", base_dir=self.dir_name)

        with chainerio.open_as_container(posix_file_path) as container:
            with container.open(self.tmpfile_path, "r") as f:
                self.assertEqual(
                    f.read(), self.test_string_str)

        chainerio.remove(zip_file_path)

    @unittest.skipIf(shutil.which('hdfs') is None, "HDFS client not installed")
    def test_fs_detection_on_container_hdfs(self):
        # Create a container for testing
        zip_file_name = "test"
        zip_file_path = zip_file_name + ".zip"

        # TODO(tianqi): add functionality ot chainerio
        from pyarrow import hdfs

        conn = hdfs.connect()
        hdfs_home = conn.info('.')['path']
        conn.close()

        hdfs_file_path = os.path.join(hdfs_home, zip_file_path)

        shutil.make_archive(zip_file_name, "zip", base_dir=self.dir_name)

        with chainerio.open(hdfs_file_path, "wb") as hdfs_file:
            with chainerio.open(zip_file_path, "rb") as posix_file:
                hdfs_file.write(posix_file.read())

        with chainerio.open_as_container(hdfs_file_path) as container:
            with container.open(self.tmpfile_path, "r") as f:
                self.assertEqual(
                    f.read(), self.test_string_str)

        chainerio.remove(zip_file_path)
        chainerio.remove(hdfs_file_path)

    def test_root_local_override(self):
        chainerio.set_root('file://' + self.dir_name)
        print(self.tmpfile_name)
        with chainerio.open(self.tmpfile_name, "r") as fp:
            self.assertEqual(fp.read(), self.test_string_str)

        # override with full URI
        with open(__file__, "r") as my_script:
            with chainerio.open('file://' + __file__) as fp:
                self.assertEqual(fp.read(), my_script.read().encode("utf-8"))

        chainerio.set_root('')

    # override with different filesystem
    @unittest.skipIf(shutil.which('hdfs') is None, "HDFS client not installed")
    def test_root_fs_override(self):
        from pyarrow import hdfs

        hdfs_tmpfile = "tmpfile_hdfs"
        hdfs_file_string = "this is a test string for hdfs"

        conn = hdfs.connect()
        with conn.open(hdfs_tmpfile, "wb") as f:
            f.write(hdfs_file_string.encode('utf-8'))

        chainerio.set_root("hdfs")
        with chainerio.open(hdfs_tmpfile, "r") as fp:
            self.assertEqual(fp.read(), hdfs_file_string)

        # override with full URI
        with open(__file__, "r") as my_script:
            with chainerio.open("file://" + __file__, "r") as fp:
                self.assertEqual(fp.read(), my_script.read())

        with chainerio.open(hdfs_tmpfile, "r") as fp:
            self.assertEqual(fp.read(), hdfs_file_string)

        conn.delete(hdfs_tmpfile)
        conn.close()

        import urllib.request
        # TODO(kuenishi): replace this with mock to prevent web access in test
        text_url = "https://www.preferred-networks.jp/"
        with urllib.request.urlopen(text_url) as validate_data:
            with chainerio.open(text_url) as http_content:
                self.assertEqual(validate_data.read(), http_content.read())

        chainerio.set_root('')

    def test_isdir(self):
        self.assertTrue(chainerio.isdir("file://" + self.dir_name))

    def test_mkdir(self):
        new_tmp_dir = "testmkdir/"
        chainerio.mkdir("file://" + new_tmp_dir)
        self.assertTrue(os.path.isdir(new_tmp_dir))
        chainerio.remove(new_tmp_dir, True)

    def test_makedirs(self):
        new_tmp_dir = "testmakedirs/"
        nested_dir = new_tmp_dir + "test_nest_dir"

        chainerio.makedirs("file://" + nested_dir)
        self.assertTrue(os.path.isdir(nested_dir))
        chainerio.remove(new_tmp_dir, True)

    def test_exists(self):
        non_exist_file = "non_exist_file"
        self.assertTrue(chainerio.exists(self.dir_name))
        self.assertFalse(chainerio.exists(non_exist_file))

    def test_rename(self):
        new_tmp_dir = "testmkdir/"
        chainerio.makedirs("file://" + new_tmp_dir)

        src = os.path.join("file://", new_tmp_dir, 'src')
        dst = os.path.join("file://", new_tmp_dir, 'dst')
        with chainerio.open(src, 'w') as fp:
            fp.write('foobar')

        chainerio.rename(src, dst)
        with chainerio.open(dst, 'r') as fp:
            data = fp.read()
            assert data == 'foobar'

        assert not chainerio.exists(src)
        assert chainerio.exists(dst)
        chainerio.remove(new_tmp_dir, True)

    def test_remove(self):
        test_file = "test_remove.txt"
        test_dir = "test_dir/"
        nested_dir = os.path.join(test_dir, "nested_file/")
        nested_file = os.path.join(nested_dir, test_file)

        with chainerio.open(test_file, 'w') as fp:
            fp.write('foobar')

        # test remove on one file
        self.assertTrue(chainerio.exists(test_file))
        chainerio.remove(test_file)
        self.assertFalse(chainerio.exists(test_file))

        # test remove on directory
        chainerio.makedirs(nested_dir)
        with chainerio.open(nested_file, 'w') as fp:
            fp.write('foobar')

        self.assertTrue(chainerio.exists(test_dir))
        self.assertTrue(chainerio.exists(nested_dir))
        self.assertTrue(chainerio.exists(nested_file))

        chainerio.remove(test_dir, True)

        self.assertFalse(chainerio.exists(test_dir))
        self.assertFalse(chainerio.exists(nested_dir))
        self.assertFalse(chainerio.exists(nested_file))

    def test_stat(self):
        # pass for now
        # TODO(tianqi) add test after we well defined the stat
        pass
