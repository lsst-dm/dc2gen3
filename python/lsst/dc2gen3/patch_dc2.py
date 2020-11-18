#!/usr/bin/env python
"""Modify a DESC DC2 to prepare it for conversion to gen3
"""

# imports

import argparse
import sys
import os
import shutil

from astropy.io import fits
import lsst.log
import lsst.log.utils

from lsst.daf.persistence import Policy
from lsst.obs.base.gen2to3.repoWalker import PathElementParser

# constants

DATATYPES_TO_CONVERT = ("flat", "sky", "SKY")
POLICY_FILE = Policy.defaultPolicyFile(
    "obs_lsst", "lsstCamMapper.yaml", "policy"
)
FIX_HEADER = False

# exception classes

# interface functions


def copy_files(origin_repo, new_repo, policy_file=POLICY_FILE):
    """Copy files from a DESC DC2 gen2 origin repo to one with modified names
    to change parsed filter names to match that expected by the gen3 butler.

    Parameters
    ==========
    origin_repo : str
        full path to origin gen2 (POSIX) filestore
    new_repo : str
        full path to destination (POSIX) filestore
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

        for old_path, new_path in _transform_pairs(
            origin_repo, new_repo, template
        ):

            if FIX_HEADER:
                assert False, "This path is untested"
                with fits.open(old_path) as hdulist:
                    for hdu in hdulist:
                        if "FILTER" in hdu.header:
                            old_filter = hdu.header["FILTER"]
                            new_filter = _transform_filter_name(old_filter)
                            hdu.header["FILTER"] = new_filter
                    hdulist.writeto(new_path)
            else:
                log.info("Copying %s to %s", old_path, new_path)
                try:
                    shutil.copyfile(old_path, new_path)
                except FileNotFoundError:
                    new_dir = os.path.split(new_path)[0]
                    try:
                        os.makedirs(new_dir)
                    except FileExistsError:
                        pass
                    shutil.copyfile(old_path, new_path)


# classes

# internal functions & classes


def _transform_filter_name(old_filter_name):
    """Create the new filter name from the old one

    Parameters
    ==========
    old_filter_name : str
        the old filter name

    Returns
    =======
    new_filter_name : str
        the new filter name

    """
    new_filter_name = old_filter_name + "_sim_1.4"
    return new_filter_name


def _transform_pairs(calib_root, new_root, template):
    """Generate tuples of old and new file names

    Parameters
    ==========
    calib_root : str
        Root directory for old (unpatched) calib POSIX datastore
    new_root : str
        Root directory of patched repo filestore
    template : str
        File name template

    Yields
    ======
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
            file_elements["filter"] = _transform_filter_name(
                file_elements["filter"]
            )
            new_file = file_template % file_elements
            old_full_path = os.path.join(old_root, old_file)
            new_full_path = os.path.join(new_root, new_path, new_file)
            yield (old_full_path, new_full_path)


def _main(args=None):
    parser = argparse.ArgumentParser(
        description="Patch a DESC DC2 gen2 butler repo for conversion to gen3"
    )
    parser.add_argument(
        "origin_repo", type=str, help="path to original DC2 repo"
    )
    parser.add_argument("new_repo", type=str, help="path to original DC2 repo")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        dest="verbose",
        default=lsst.log.Log.INFO,
        const=lsst.log.Log.DEBUG,
        help="Set the log level to DEBUG.",
    )

    options = parser.parse_args(args)

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
    log.setLevel(options.verbose)

    copy_files(options.origin_repo, options.new_repo)
    return 0


if __name__ == "__main__":
    result = _main()
    sys.exit(result)
