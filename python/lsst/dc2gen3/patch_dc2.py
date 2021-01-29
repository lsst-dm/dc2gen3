#!/usr/bin/env python
"""Modify a DESC DC2 gen2 butler to prepare it for conversion to gen3
"""

# imports

import argparse
import sys
import os
import shutil
import sqlite3
import pathlib
import tempfile
import yaml
from contextlib import closing

import lsst.log
import lsst.log.utils

from lsst.daf.persistence import Policy
from lsst.obs.base.gen2to3.repoWalker import PathElementParser

# constants

DATATYPES_TO_CONVERT = ("flat", "sky", "SKY")
POLICY_FILE = Policy.defaultPolicyFile("obs_lsst", "lsstCamMapper.yaml", "policy")

# exception classes

# interface functions


def move_files(calib_root, policy_file=POLICY_FILE):
    """Copy files from a DESC DC2 gen2 origin repo to one with modified names
    to change parsed filter names to match that expected by the gen3 butler.

    Parameters
    ----------
    calib_root : str
        full path to the calib gen2 (POSIX) filestore
    policy_file : str
        Policy file from which to load templates

    """
    log = lsst.log.Log.getLogger("convertRepo")
    policy = Policy(policy_file)
    for data_type in DATATYPES_TO_CONVERT:

        if data_type == "SKY":
            template = policy["calibrations"]["sky"]["template"]
            template = template.replace("sky", "SKY")
        else:
            template = policy["calibrations"][data_type]["template"]

        for old_filename, new_filename in _transform_pairs(calib_root, template):
            log.info("Moving %s to %s", old_filename, new_filename)
            new_dir = os.path.split(new_filename)[0]
            try:
                os.makedirs(new_dir)
            except FileExistsError:
                pass
            shutil.move(old_filename, new_filename)


def update_registry(gen2_root):
    """Update a gen2 registry to include new columns

    Parameters
    ----------
    gen2_root : str
        full path to the calib gen2 (POSIX) filestore
    """
    log = lsst.log.Log.getLogger("convertRepo")

    registry_filename = os.path.join(gen2_root, "registry.sqlite3")
    _break_hardlink(registry_filename)

    with closing(sqlite3.connect(registry_filename)) as con:
        raw_columns = [c[1] for c in con.execute("PRAGMA table_info(raw);")]

        if "controller" not in raw_columns:
            with con:
                con.execute("ALTER TABLE raw ADD COLUMN controller TEXT;")
                con.execute("UPDATE raw SET controller = 'S'")
                log.info(
                    f"Added 'controller' column to raw table of {registry_filename}"
                )

        if "obsid" not in raw_columns:
            with con:
                con.execute("ALTER TABLE raw ADD COLUMN obsid TEXT;")
                con.execute("UPDATE raw SET obsid=visit")
                log.info(f"Added 'obsid' column to raw table of {registry_filename}")

        if "expGroup" not in raw_columns:
            with con:
                con.execute("ALTER TABLE raw ADD COLUMN expGroup TEXT;")
                con.execute("UPDATE raw SET expGroup=CAST(visit AS TEXT)")
                log.info(f"Added 'expGroup' column to raw table of {registry_filename}")

        if "expId" not in raw_columns:
            with con:
                con.execute("ALTER TABLE raw ADD COLUMN expId INT;")
                con.execute("UPDATE raw SET expId=visit")
                log.info(f"Added 'expId' column to raw table of {registry_filename}")

        uraw_index_query = (
            "SELECT * FROM sqlite_master WHERE type='index' AND name='u_raw';"
        )
        has_uraw_index = len([r for r in con.execute(uraw_index_query)]) > 0
        if not has_uraw_index:
            with con:
                con.execute(
                    "CREATE UNIQUE INDEX u_raw ON raw (expId, detector, visit);"
                )
                log.info(f"Added 'u_raw' index to raw table of {registry_filename}")


def update_calib_registry(calib_root):
    """Update a gen2 calib registry to use new filter names

    Parameters
    ----------
    calib_root : str
        full path to the calib gen2 (POSIX) filestore
    """
    log = lsst.log.Log.getLogger("convertRepo")
    tables_to_update = (
        "flat",
        "flat_visit",
        "fringe",
        "fringe_visit",
        "sky",
        "sky_visit",
    )
    old_filter_names = ("u", "g", "r", "i", "z", "y")

    calib_registry_filename = os.path.join(calib_root, "calibRegistry.sqlite3")
    _break_hardlink(calib_registry_filename)

    with closing(sqlite3.connect(calib_registry_filename)) as con:
        for table in tables_to_update:
            for old_filter_name in old_filter_names:
                new_filter_name = _transform_filter_name(old_filter_name)
                query = f"UPDATE {table} SET filter='{new_filter_name}' WHERE filter='{old_filter_name}';"
                log.debug(f"Executing query on {calib_registry_filename}: {query}")
                with con:
                    con.execute(query)
                    # Split the message into several lines, which
                    # makes the code worse but the linter happy.
                    log.info(
                        f"Changed old filter name '{old_filter_name}'"
                        + f" in table {table} of {calib_registry_filename}"
                        + f" to '{new_filter_name}'"
                    )


def replace_root(filename, old_root, new_root):
    """Replace the original gen2 root with the new one

    Parameters
    ----------
    filename : `str`
        the path of the file in which to replace the root
    old_root : `str`
        the original fully qualified path of the gen2 repo root
    new_root : `str`
        the new fully qualified path of the gen2 repo root
    """
    log = lsst.log.Log.getLogger("convertRepo")

    # Not attempting to deal with parsing the yaml, because
    # that would require building a special purpose constructor
    # and emitter for RepositoryCfg_v1, which would only
    # make it more fragile.

    with open(filename, "r") as f:
        config_str = f.read()

    new_config_str = config_str.replace(old_root, new_root)
    if new_config_str == config_str:
        log.info(f"No change in {filename}")
    else:
        _break_hardlink(filename)
        with open(filename, "w") as f:
            f.write(new_config_str)
        log.info(f"{filename} updated")


# classes

# internal functions & classes


def _break_hardlink(filename):
    """Break a hardlink between a file and any others, if necessary

    Parameters
    ----------
    filename : `str`
        the path of the file
    """
    # If it isn't hardlinked to anything else, there is nothing to do
    if os.stat(filename).st_nlink <= 1:
        return

    parent_dir = pathlib.Path(filename).parent.absolute()
    with tempfile.TemporaryDirectory(dir=parent_dir) as temp_dir:
        temp_file = os.path.join(temp_dir, "linked_file")
        shutil.move(filename, temp_file)

        try:
            shutil.copy(temp_file, filename)
            assert (
                os.stat(filename).st_nlink == 1
            ), f"Failed to break hardlink on {filename}"

            # the linter does not like a bare except, but
            # I really do want to clean up no matter what
            # the exception was, so catch the base of
            # all exceptions.
        except BaseException as error:
            # If for any reason we could not make the
            # new copy, try to put the old one back before
            # cleaning up the temp directory
            if not os.path.isfile(filename):
                shutil.move(temp_file, filename)
            raise error


def _transform_filter_name(old_filter_name):
    """Create the new filter name from the old one

    Parameters
    ----------
    old_filter_name : str
        the old filter name

    Returns
    -------
    new_filter_name : str
        the new filter name

    """
    new_filter_name = old_filter_name + "_sim_1.4"
    return new_filter_name


def _transform_pairs(calib_root, template):
    """Generate tuples of old and new file names

    Parameters
    ----------
    calib_root : str
        Root directory for the gen2 POSIX datastore
    template : str
        File name template

    Yields
    ------
    old_file_path : `str`
        the path of the file in the old POSIX datastore
    new_file_path : `str`
        the path of the file in the old POSIX datastore
    """
    dir_template, file_template = os.path.split(template)
    full_dir_template = os.path.join(calib_root, dir_template)
    dir_keys = {"filter": str, "calibDate": str}
    dir_parser = PathElementParser(full_dir_template, dir_keys)
    file_keys = {
        "filter": str,
        "raftName": str,
        "detectorName": str,
        "calibDate": str,
        "detector": int,
    }
    file_parser = PathElementParser(file_template, file_keys)
    for old_root, old_dirs, old_files in os.walk(calib_root):
        # Extract the metadata in the path
        dir_elements = {}
        dir_elements = dir_parser.parse(old_root, dir_elements)

        # Ignore directories that do not fit the template
        if not dir_elements:
            continue

        # Update the metadata for the path
        dir_elements["filter"] = _transform_filter_name(dir_elements["filter"])
        new_path = dir_template % dir_elements

        # Process the files
        for old_file in old_files:
            file_elements = {}
            file_elements = file_parser.parse(old_file, file_elements)

            # Ignore files that do not fit the template
            if not file_elements:
                continue
            file_elements["filter"] = _transform_filter_name(file_elements["filter"])
            new_file = file_template % file_elements
            old_full_path = os.path.join(old_root, old_file)
            new_full_path = os.path.join(calib_root, new_path, new_file)
            yield (old_full_path, new_full_path)


def _main(args=None):
    parser = argparse.ArgumentParser(
        description="Patch a DESC DC2 gen2 butler repo for conversion to gen3"
    )
    parser.add_argument(
        "option_filename", type=str, help="file from which to load options"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        dest="verbose",
        default=lsst.log.Log.INFO,
        const=lsst.log.Log.DEBUG,
        help="Set the log level to DEBUG.",
    )

    commandline_options = parser.parse_args(args)

    lsst.log.configure_prop(
        """
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.out
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern=%-5p %d{yyyy-MM-ddTHH:mm:ss.SSSZ} %c - %m%n
"""
    )
    log = lsst.log.Log.getLogger("convertRepo")
    log.setLevel(commandline_options.verbose)

    with open(commandline_options.option_filename, "r") as options_file:
        options = yaml.safe_load(options_file)

    origin_root = options["origin_root"]
    gen2_root = options["gen2_root"]
    rerun_paths = [os.path.join(gen2_root, r["path"]) for r in options["reruns"]]
    calib_paths = [os.path.join(gen2_root, c["path"]) for c in options["calibs"]]
    repository_paths = rerun_paths + calib_paths

    for repository_path in repository_paths:
        filename = os.path.join(repository_path, "repositoryCfg.yaml")
        replace_root(filename, origin_root, gen2_root)

    calib_root = os.path.join(gen2_root, "CALIB")
    for calib_root in calib_paths:
        move_files(calib_root)
        update_calib_registry(calib_root)

    update_registry(gen2_root)

    return 0


if __name__ == "__main__":
    result = _main()
    sys.exit(result)
