import os
import unittest
from unittest.mock import patch, MagicMock
from tempfile import TemporaryDirectory

import lsst.log
import lsst.obs.base
import lsst.obs.base.gen2to3
from lsst.daf.butler import Butler

from lsst.dc2gen3 import dc2gen3
from lsst.dc2gen3 import DC2Converter

# If tests were under python/dc2gen3, I could use importlib.resources
# to find the test config files, but DM standards want the test
# directory to be a sibling of the python dir, not a descendent
TEST_WORKING_DIR = os.path.join(os.path.dirname(__file__), 'config')
TEST_CONFIG_FILE = 'convert_options.yaml'


class DC2ConverterTests(unittest.TestCase):
    old_cwd = None

    def setUp(self):
        self.old_cwd = os.getcwd()
        os.chdir(TEST_WORKING_DIR)

    def tearDown(self):
        os.chdir(self.old_cwd)

    def test_load(self, *mocks):
        converter = DC2Converter.load(TEST_CONFIG_FILE)
        self.assertIsInstance(converter.gen2_root, str)
        self.assertIsInstance(converter.gen3_root, str)
        self.assertIsInstance(converter.instrument, lsst.obs.base.Instrument)
        self.assertIsInstance(
            converter.convert_repo_config,
            lsst.obs.base.gen2to3.ConvertRepoTask.ConfigClass)
        self.assertIsInstance(converter.butler_seed, str)
        self.assertIsInstance(converter.reruns, list)
        self.assertIsInstance(converter.reruns[0], lsst.obs.base.gen2to3.Rerun)
        self.assertIsInstance(converter.calibs, dict)
        self.assertIsInstance(converter.delete_old_gen3, bool)
        self.assertIsInstance(converter.create_new_gen3, bool)
        self.assertIsInstance(converter.convert_skymap, bool)

    @patch('lsst.dc2gen3.dc2gen3.Butler', spec=True)
    def test_make_gen3_butler(self, *mocks):
        converter = DC2Converter.load(TEST_CONFIG_FILE)
        dc2gen3.Butler()
        butler = converter.gen3_butler
        self.assertIsInstance(butler, Butler)
        second_butler = converter.gen3_butler
        self.assertIs(second_butler, butler)

    @patch('lsst.dc2gen3.dc2gen3.Butler', spec=True)
    @patch('lsst.dc2gen3.dc2gen3.ConvertRepoTask', spec=True)
    def test_run_conversion_task(self, *mocks):
        converter = DC2Converter.load(TEST_CONFIG_FILE)
        task = converter.run_conversion_task()
        self.assertIsInstance(converter.gen3_butler, Butler)
        task.run.assert_called_once()

    @patch('lsst.dc2gen3.dc2gen3.Butler', spec=True)
    @patch('lsst.dc2gen3.dc2gen3.ConvertRepoTask', spec=True)
    @patch('lsst.dc2gen3.dc2gen3._empty_registry', spec=True)
    @patch('lsst.dc2gen3.dc2gen3._delete_data', spec=True)
    def test_convert_new_no_delete(self, *mocks):
        converter = DC2Converter.load(TEST_CONFIG_FILE)
        converter.gen3_root, parent_dir = make_name_of_nonexistant_dir()
        task = converter.convert_new(delete=False)
        self.assertIsInstance(converter.gen3_butler, Butler)
        task.run.assert_called_once()
        dc2gen3.Butler.makeRepo.assert_called_once()
        dc2gen3._empty_registry.assert_not_called()
        dc2gen3._delete_data.assert_not_called()

    @patch('lsst.dc2gen3.dc2gen3.Butler', spec=True)
    @patch('lsst.dc2gen3.dc2gen3.ConvertRepoTask', spec=True)
    @patch('lsst.dc2gen3.dc2gen3._empty_registry', spec=True)
    @patch('lsst.dc2gen3.dc2gen3._delete_data', spec=True)
    def test_convert_with_delete(self, *mocks):
        converter = DC2Converter.load(TEST_CONFIG_FILE)
        converter.gen3_root, parent_dir = make_name_of_nonexistant_dir()
        task = converter.convert_new(delete=True)
        self.assertIsInstance(converter.gen3_butler, Butler)
        task.run.assert_called_once()
        dc2gen3.Butler.makeRepo.assert_called_once()
        dc2gen3._empty_registry.assert_called_once()
        dc2gen3._delete_data.assert_called_once()

    @patch('lsst.dc2gen3.dc2gen3.DC2Converter.convert_new', spec=True)
    @patch('lsst.dc2gen3.dc2gen3.DC2Converter.run_conversion_task', spec=True)
    def test_converter_call_create(self, *mocks):
        converter = DC2Converter.load(TEST_CONFIG_FILE)
        converter.create_new_gen3 = False
        converter()
        converter.run_conversion_task.assert_called_once()
        converter.convert_new.assert_not_called()

    @patch('lsst.dc2gen3.dc2gen3.DC2Converter.convert_new', spec=True)
    @patch('lsst.dc2gen3.dc2gen3.DC2Converter.run_conversion_task', spec=True)
    def test_converter_call_without_create(self, *mocks):
        converter = DC2Converter.load(TEST_CONFIG_FILE)
        converter.create_new_gen3 = True
        converter()
        converter.convert_new.assert_called_once()
        converter.run_conversion_task.assert_not_called()

    @patch('lsst.dc2gen3.dc2gen3._empty_registry', spec=True)
    @patch('lsst.dc2gen3.dc2gen3._delete_data', spec=True)
    def test_delete_gen3_repo(self, *mocks):
        converter = DC2Converter.load(TEST_CONFIG_FILE)
        connection_uri = 'postgresql:///bogus?host=example.org&port=0&user=nobody'
        registry_namespace = 'bogusnamespace'
        converter.gen3_root, parent_dir = make_name_of_nonexistant_dir()
        converter._delete_gen3_repo(connection_uri,
                                    registry_namespace)
        dc2gen3._empty_registry.assert_called_once()
        dc2gen3._delete_data.assert_called_once()

    @patch('lsst.dc2gen3.dc2gen3.DatasetType', spec=True)
    @patch('lsst.dc2gen3.dc2gen3.Butler')
    def test_put_skymap(self, *macks):
        converter = DC2Converter.load(TEST_CONFIG_FILE)
        converter.instrument = MagicMock(spec=converter.instrument)
        converter._put_skymap()
        converter.gen3_butler.put.assert_called_once()

    def test_registry_connection_uri(self):
        converter = DC2Converter.load(TEST_CONFIG_FILE)
        uri = converter.registry_connection_uri
        self.assertIsInstance(uri, str)

    def test_registry_namespace(self):
        converter = DC2Converter.load(TEST_CONFIG_FILE)
        registry_namespace = converter.registry_namespace
        self.assertIsInstance(registry_namespace, str)

    @patch('builtins.input', lambda *args: 'y')
    @patch('psycopg2.connect', spec=True)
    def test_empty_registry(self, *mocks):
        connection_uri = 'postgresql:///bogus?host=example.org&port=0&user=nobody'
        test_namespace = 'bogusnamespace'
        dc2gen3._empty_registry(connection_uri, test_namespace)

    @patch('builtins.input', lambda *args: 'y')
    @patch('lsst.dc2gen3.dc2gen3.shutil.rmtree', spec=True)
    def test_delete_data(self, *mocks):
        test_root, parent_dir = make_name_of_nonexistant_dir()
        dc2gen3._delete_data(test_root)
        dc2gen3.shutil.rmtree.assert_called_once()

    @patch('lsst.log', spec=True)
    def test_configure_logging(self, *mocks):
        log_level = 1
        dc2gen3._configure_logging(log_level)
        lsst.log.configure_prop.assert_called_once()

    @patch('argparse.ArgumentParser', spec=True)
    @patch('lsst.dc2gen3.dc2gen3.DC2Converter', spec=True)
    def test_main(self, *mocks):
        dc2gen3.main()
        dc2gen3.DC2Converter.load.assert_called_once()


def make_name_of_nonexistant_dir():
    my_temp_dir = TemporaryDirectory()
    dir_cannot_exist = os.path.join(
        my_temp_dir.name, 'doesnt_exist')
    # pass back the parent, so it doesn't go out of scope
    # and get cleaned up until after we've "used" it.
    return dir_cannot_exist, my_temp_dir
