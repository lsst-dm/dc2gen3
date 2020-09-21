DESC DC2 gen2 to gen3 butler conversion script
==============================================

The `dc2gen3` script was written primarily as an exercise in
understanding the API for the `lsst.obs.base.gen2to3.CovertRepoTask`
LSST stack module, but with the hope that it will be useful for
benchmarking the code to test how fast the IDF DP0 repository can be
loaded with the DESC DC2 data (or a subset thereof) (See
[PREOPS-102](https://jira.lsstcorp.org/browse/PREOPS-102).)

One options file and two configuration files drive the behaviour of
the script. The options file, an example of whic can be found in
`config/convert_options.yaml`, contains options that may vary from one
execution to another, including which reruns and visits are to be
converted. This options file alse contains references to the two
configuration files to be used: the seed configuration for the gen3
repository (see `config/butler_seed.yaml` for an example), and the pex
config task configuration for the `convertRepo` task itself (see
`config/convertrepo.pexconfig` for an example).

If `$MY_LSST` and `$MY_DEVEL_DIR` are the directories where your LSST
stack and desired parent of the checked-out product directory,
repsectively, then to check out and build `dc2gen3`:

```
source ${MY_LSST}/loadLSST.bash
cd ${MY_DEVEL_DIR}
git clone git@github.com:lsst/dc2gen3.git
cd dc2gen3
setup -r $(pwd) dc2gen3
scons
```

To actually run it, copy the example options and config files in to a
working directory, edit them to refect what you want to do, and run
`dc2gen3` thus:

```
cd ${MY_WORKING_DIR}
cp ${DC2GEN3_DIR}/config/* .
# Edit convertrepo.pexconfig, butler_seed.yaml, and convert_options.yaml
dc2gen3 convert_options.yaml
```

If you want to delete an old gen3 repo at the configured location
(completely removing all database tables and data files) before doing
the conversion, set the `delete_old_gen3` option in the
`convert_options.yaml` file to `True`, and also add the `--delete`
option to the call itself, such that it becomes:

```
dc2gen3 convert_options.yaml --delete
```


