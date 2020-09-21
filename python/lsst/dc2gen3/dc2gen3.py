#!/usr/bin/env python
"""Driver script to convert a DESC DC2 gen2 butler repository to a gen3 one.
"""

# imports

from __future__ import annotations

import shutil
import os
import sys
import argparse
from typing import Iterable, Optional, List, Dict
from dataclasses import dataclass, field

import yaml
import psycopg2

import lsst.log
import lsst.log.utils
from lsst.utils import doImport
from lsst.obs.base.gen2to3 import ConvertRepoTask
from lsst.obs.base.gen2to3 import Rerun
from lsst.daf.butler import Butler, DatasetType
from lsst.daf.butler.registry import ConflictingDefinitionError
from lsst.pipe.tasks.makeSkyMap import MakeSkyMapTask

# constants

# This is stupid, but I don't see another way to
# get it other than accessing "private" members.
CONVERT_REPO_TASK_NAME = ConvertRepoTask._DefaultName

# exception classes

# interface functions

# classes


@dataclass(order=True)
class DC2Converter():
    """Manager of conversion of DC2 from gen2 to gen3.
    """
    # pylint: disable=too-many-instance-attributes
    gen2_root: str
    gen3_root: str
    instrument: lsst.obs.base.Instrument = field(hash=False)
    convert_repo_config: ConvertRepoTask.ConfigClass = field(hash=False)
    reruns: List[Rerun] = field(hash=False)
    calibs: Dict[str, str] = field(hash=False, default=None)
    visits: Optional[Iterable[int]] = field(hash=False, default=None)
    delete_old_gen3: bool = False
    create_new_gen3: bool = False
    convert_skymap: bool = False
    butler_seed: Optional[str] = None
    _gen3_butler: Optional[Butler] = field(hash=False, default=None)

    # The __init__ method is implicitly created by the @dataclass decorator

    @classmethod
    def load(cls, param_fname):
        """Instantiate using values from a yaml file.

        Parameters
        ----------
        param_fname : str
            The file name of the yaml file from which configuration
            is to be read

        Returns
        -------
        converter : DC2Converter
            An instance of the DC2Converter
        """
        with open(param_fname, 'r') as file_handle:
            params = yaml.safe_load(file_handle)

        # Build the arguments for the default constructor
        init_kwargs = {}

        # Convert strings in the yaml file to instantiated objects
        init_kwargs['reruns'] = [Rerun(**k) for k in params['reruns']]

        instrument_class = doImport(params['instrument'])
        instrument = instrument_class()
        init_kwargs['instrument'] = instrument

        task_config = ConvertRepoTask.ConfigClass()
        instrument.applyConfigOverrides(
            CONVERT_REPO_TASK_NAME,
            task_config)
        task_config.load(params['convertRepo_config'])
        init_kwargs['convert_repo_config'] = task_config

        init_kwargs['calibs'] = {}
        for key, value in params['calibs'].items():
            collection_name = instrument.makeCollectionName(value)
            init_kwargs['calibs'][key] = collection_name

        # Everything that didn't need processing, assign directly
        unmapped = ('convertRepo_config',)
        for key in params:
            if key not in init_kwargs:
                if key not in unmapped:
                    init_kwargs[key] = params[key]

        new_instance = cls(**init_kwargs)

        return new_instance

    @property
    def registry_connection_uri(self):
        """The registry database connection URI

        Returns
        -------
        registry_uri : str
            The gen3 registry URI
        """
        with open(self.butler_seed) as file_handle:
            seed_config = yaml.load(file_handle, Loader=yaml.FullLoader)

        connection_uri = seed_config['registry']['db']
        return connection_uri

    @property
    def registry_namespace(self):
        """The registry database namespace (schema in the database)

        Returns
        -------
        namespace : str
            The gen3 registry namespace (schema in the database)
        """
        with open(self.butler_seed) as file_handle:
            seed_config = yaml.load(file_handle, Loader=yaml.FullLoader)

        namespace = seed_config['registry']['namespace']
        return namespace

    @property
    def gen3_butler(self):
        """Return an instance of the gen3 butler, instantiating if needed.

        Returns
        -------
        butler : Butler
            An instance of the configured gen3 butler, reused if possible,
            created if needed.
        """
        if self._gen3_butler is None:
            lsst.log.info("Instantiating the gen3 Butler")
            self._gen3_butler = Butler(
                self.gen3_root,
                run=self.instrument.makeDefaultRawIngestRunName())

        return self._gen3_butler

    def run_conversion_task(self):
        """Create and execute a gen2 to gen3 conversion task

        Returns
        -------
        task: ConvertRepoTask
            the completed task object
        """

        lsst.log.info("Instantiating ConvertRepoTask")
        task = ConvertRepoTask(config=self.convert_repo_config,
                               butler3=self.gen3_butler,
                               instrument=self.instrument,
                               name=CONVERT_REPO_TASK_NAME)

        lsst.log.info(f"ConvertRepoTask.run argument root = {self.gen2_root}")
        lsst.log.info(f"ConvertRepoTask.run argument reruns = {self.reruns}")
        lsst.log.info(f"ConvertRepoTask.run argument calibs = {self.calibs}")
        visits_log_msg = f"ConvertRepoTask.run argument visits = {self.visits}"
        if len(visits_log_msg) <= 80:
            lsst.log.info(visits_log_msg)
        else:
            lsst.log.info(visits_log_msg[:77] + '...')

        task.run(root=self.gen2_root,
                 reruns=self.reruns,
                 calibs=self.calibs,
                 visits=self.visits)

        lsst.log.info("Finished running the ConvertRepoTask")
        return task

    def convert_new(self, delete: bool = False):
        """Create or empty a gen3 a repo and convert a gen2 repo

        Parameters
        ----------
        delete : bool
            True if you really want to delete the old gen3 registry
            and data store

        Returns
        -------
        task: ConvertRepoTask
            the completed task object
        """
        if delete:
            self._delete_gen3_repo(self.registry_connection_uri,
                                   self.registry_namespace)

        if os.path.exists(self.gen3_root):
            raise ValueError("Cannot create because repo dir exists.")

        Butler.makeRepo(self.gen3_root, config=self.butler_seed)

        task = self.run_conversion_task()

        if self.convert_skymap:
            try:
                self._put_skymap()
            except ConflictingDefinitionError:
                msg = "Cannot add skyMap; "
                msg += "maybe it already exists from gen2 conversion?"
                lsst.log.info(msg)
                raise

        return task

    def __call__(self, delete: bool = False):
        if self.create_new_gen3:
            task = self.convert_new(delete)
        else:
            assert not delete
            task = self.run_conversion_task()

        return task

    def _delete_gen3_repo(self, connection_uri, namespace):
        assert self.delete_old_gen3
        self._gen3_butler = None
        _empty_registry(connection_uri, namespace)
        _delete_data(self.gen3_root)

    def _put_skymap(self):
        lsst.log.info("Starting to insert the skyMap")
        dataset_type = DatasetType(
            name="deepCoadd_skyMap",
            dimensions=["skymap"],
            storageClass="SkyMap",
            universe=self.gen3_butler.registry.dimensions)
        self.gen3_butler.registry.registerDatasetType(dataset_type)
        run = "skymaps"
        self.gen3_butler.registry.registerRun(run)
        skymap_config = MakeSkyMapTask.ConfigClass()
        self.instrument.applyConfigOverrides(
            MakeSkyMapTask._DefaultName, skymap_config)
        skymap = skymap_config.skyMap.apply()
        self.gen3_butler.put(skymap, dataset_type, skymap="rings", run=run)
        lsst.log.info("Finished inserting the skyMap")

# internal functions & classes


def _configure_logging(level):
    lsst.log.configure_prop("""
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.out
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern=%-5p %d{yyyy-MM-ddTHH:mm:ss.SSSZ} %c - %m%n
""")
    log = lsst.log.Log.getLogger("convertRepo")
    log.setLevel(level)


def _empty_registry(connection_uri, namespace):
    print(f'About to delete all tables in schema {namespace} from {connection_uri}.')
    approved = input("Are you sure (y/n)? ") == 'y'
    if not approved:
        sys.exit(1)

    sql_tab = f"""SELECT tablename FROM pg_tables
                  WHERE schemaname = '{namespace}'"""
    sql_seq = f"""SELECT c.relname relname
                   FROM pg_class c,
                        pg_namespace n
                   WHERE n.oid = c.relnamespace
                     AND c.relkind = 'S'
                     AND n.nspname = '{namespace}'"""
    with psycopg2.connect(connection_uri) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_tab)
            tables = cur.fetchall()
            for table in tables:
                lsst.log.info(f"dropping table {namespace}.{table[0]}")
                cur.execute(f"DROP TABLE IF EXISTS {namespace}.{table[0]} CASCADE")
        with conn.cursor() as cur:
            cur.execute(sql_seq)
            seqs = cur.fetchall()
            for seq in seqs:
                lsst.log.info(f"dropping sequence {namespace}.{seq[0]}")
                cur.execute(f"DROP SEQUENCE IF EXISTS {namespace}.{seq[0]} CASCADE")


def _delete_data(gen3_root):
    print(f'About to recursively delete directory {gen3_root}.')
    approved = input("Are you sure (y/n)? ") == 'y'
    if not approved:
        sys.exit(1)

    try:
        shutil.rmtree(gen3_root)
    except FileNotFoundError:
        pass


def main():
    parser = argparse.ArgumentParser(
        description="Bootstrap a Gen3 Butler data repository with DC2 imsim2.2i data."
    )
    parser.add_argument(
        "params", type=str,
        help="yaml file with conversion parameters")
    parser.add_argument(
        "--delete", action="store_true", default=False,
        help="Delete the gen 3 repo and its contents before attempting to populating it.")
    parser.add_argument(
        "-v", "--verbose", action="store_const", dest="verbose",
        default=lsst.log.Log.INFO, const=lsst.log.Log.DEBUG,
        help="Set the log level to DEBUG.")
    options = parser.parse_args()
    _configure_logging(options.verbose)

    converter = DC2Converter.load(options.params)
    converter(options.delete)
    return 0


if __name__ == "__main__":
    result = main()
    sys.exit(result)
