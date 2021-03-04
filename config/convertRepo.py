from lsst.obs.base.gen2to3 import ConvertRepoSkyMapConfig

# How to transfer files (None for no transfer).
config.raws.transfer = 'direct'

# If True (default), only convert datasets that are related to the
# ingested visits.  Ignored unless a list of visits is passed to run().
config.relatedOnly = True

# Glob-style patterns for dataset type names that should not be converted
# despite matching a pattern in datasetIncludePatterns.

config.datasetIgnorePatterns = ['raw', '_raw', 'raw_hdu', 'raw_amp',
                                '*_camera', 'singleFrameDriver_metadata']

# Filename globs that should be ignored instead of being treated as datasets.
config.fileIgnorePatterns = ['README.txt', '*~?', 'butler.yaml',
                             'gen3.sqlite3', 'registry.sqlite3',
                             'calibRegistry.sqlite3', '_mapper',
                             '_parent', 'repositoryCfg.yaml',
                             '*.log', '*.png', 'rerun*']

# If True (default), add dimension records for the Instrument and
# its filters and detectors to the registry instead of assuming
# they are already present.
config.doRegisterInstrument = False

# Repeat the configs used to create DC2:
# https://github.com/LSSTDESC/gen3_workflow/blob/u/jchiang/gen3_scripts/config/makeSkyMap.py
config.skyMaps["DC2"] = ConvertRepoSkyMapConfig()
config.skyMaps["DC2"].skyMap.name = "rings"
config.skyMaps["DC2"].skyMap["rings"].numRings = 120
config.skyMaps["DC2"].skyMap["rings"].projection = "TAN"
config.skyMaps["DC2"].skyMap["rings"].tractOverlap = 1.0/60  # Overlap between tracts (degrees)
config.skyMaps["DC2"].skyMap["rings"].pixelScale = 0.2
