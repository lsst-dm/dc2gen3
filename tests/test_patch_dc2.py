import os
import unittest
from unittest.mock import patch

from lsst.dc2gen3 import patch_dc2

TEST_CALIB_OLD_ROOT = "/tmp/test_patch_dc2_old_calib_root"
TEST_CALIB_NEW_ROOT = "/tmp/test_patch_dc2_new_calib_root"

# Test data with which to patch os.walk,
# so we don't need to walk a real filesystem
TEST1_OS_WALK = [
    (TEST_CALIB_OLD_ROOT + "/bias", ["2022-01-01"], []),
    (
        TEST_CALIB_OLD_ROOT + "/bias/2022-01-01",
        [],
        [
            "bias-R11-S00-det036_2022-01-01.fits",
            "bias-R01-S00-det000_2022-01-01.fits",
            "bias-R10-S22-det035_2022-01-01.fits",
            "bias-R01-S01-det001_2022-01-01.fits",
            "bias-R11-S01-det037_2022-01-01.fits",
        ],
    ),
    (TEST_CALIB_OLD_ROOT + "/flat/g", ["2022-08-06"], []),
    (
        TEST_CALIB_OLD_ROOT + "/flat/g/2022-08-06",
        [],
        [
            "flat_g-R10-S10-det030_2022-08-06.fits",
            "flat_g-R01-S00-det000_2022-08-06.fits",
            "flat_g-R10-S11-det031_2022-08-06.fits",
            "flat_g-R01-S01-det001_2022-08-06.fits",
            "flat_g-R10-S12-det032_2022-08-06.fits",
        ],
    ),
    (TEST_CALIB_OLD_ROOT + "/flat/y", ["2022-08-06"], []),
    (
        TEST_CALIB_OLD_ROOT + "/flat/y/2022-08-06",
        [],
        [
            "flat_y-R10-S10-det030_2022-08-06.fits",
            "flat_y-R01-S00-det000_2022-08-06.fits",
            "flat_y-R10-S11-det031_2022-08-06.fits",
            "flat_y-R01-S01-det001_2022-08-06.fits",
            "flat_y-R10-S12-det032_2022-08-06.fits",
        ],
    ),
]

TEST1_TRANSFORM_PAIRS = (
    (
        TEST_CALIB_OLD_ROOT
        + "/flat/g/2022-08-06/flat_g-R10-S10-det030_2022-08-06.fits",
        TEST_CALIB_NEW_ROOT
        + "/flat/g_sim_1.4/2022-08-06/flat_g_sim_1.4-R10-S10-det030_2022-08-06.fits",
    ),
    (
        TEST_CALIB_OLD_ROOT
        + "/flat/g/2022-08-06/flat_g-R01-S00-det000_2022-08-06.fits",
        TEST_CALIB_NEW_ROOT
        + "/flat/g_sim_1.4/2022-08-06/flat_g_sim_1.4-R01-S00-det000_2022-08-06.fits",
    ),
    (
        TEST_CALIB_OLD_ROOT
        + "/flat/g/2022-08-06/flat_g-R10-S11-det031_2022-08-06.fits",
        TEST_CALIB_NEW_ROOT
        + "/flat/g_sim_1.4/2022-08-06/flat_g_sim_1.4-R10-S11-det031_2022-08-06.fits",
    ),
    (
        TEST_CALIB_OLD_ROOT
        + "/flat/g/2022-08-06/flat_g-R01-S01-det001_2022-08-06.fits",
        TEST_CALIB_NEW_ROOT
        + "/flat/g_sim_1.4/2022-08-06/flat_g_sim_1.4-R01-S01-det001_2022-08-06.fits",
    ),
    (
        TEST_CALIB_OLD_ROOT
        + "/flat/g/2022-08-06/flat_g-R10-S12-det032_2022-08-06.fits",
        TEST_CALIB_NEW_ROOT
        + "/flat/g_sim_1.4/2022-08-06/flat_g_sim_1.4-R10-S12-det032_2022-08-06.fits",
    ),
    (
        TEST_CALIB_OLD_ROOT
        + "/flat/y/2022-08-06/flat_y-R10-S10-det030_2022-08-06.fits",
        TEST_CALIB_NEW_ROOT
        + "/flat/y_sim_1.4/2022-08-06/flat_y_sim_1.4-R10-S10-det030_2022-08-06.fits",
    ),
    (
        TEST_CALIB_OLD_ROOT
        + "/flat/y/2022-08-06/flat_y-R01-S00-det000_2022-08-06.fits",
        TEST_CALIB_NEW_ROOT
        + "/flat/y_sim_1.4/2022-08-06/flat_y_sim_1.4-R01-S00-det000_2022-08-06.fits",
    ),
    (
        TEST_CALIB_OLD_ROOT
        + "/flat/y/2022-08-06/flat_y-R10-S11-det031_2022-08-06.fits",
        TEST_CALIB_NEW_ROOT
        + "/flat/y_sim_1.4/2022-08-06/flat_y_sim_1.4-R10-S11-det031_2022-08-06.fits",
    ),
    (
        TEST_CALIB_OLD_ROOT
        + "/flat/y/2022-08-06/flat_y-R01-S01-det001_2022-08-06.fits",
        TEST_CALIB_NEW_ROOT
        + "/flat/y_sim_1.4/2022-08-06/flat_y_sim_1.4-R01-S01-det001_2022-08-06.fits",
    ),
    (
        TEST_CALIB_OLD_ROOT
        + "/flat/y/2022-08-06/flat_y-R10-S12-det032_2022-08-06.fits",
        TEST_CALIB_NEW_ROOT
        + "/flat/y_sim_1.4/2022-08-06/flat_y_sim_1.4-R10-S12-det032_2022-08-06.fits",
    ),
)

# Do not declare with a string, because it is
# long enough to make flake8 unhappy
TEST_TEMPLATE = os.path.join(
    "flat",
    "%(filter)s",
    "%(calibDate)s",
    "flat_%(filter)s-%(raftName)s-%(detectorName)s-det%(detector)"
    + "03d_%(calibDate)s.fits",
)


@patch("os.walk", return_value=TEST1_OS_WALK, spec=True)
@patch("shutil.copyfile", spec=True)
class PatchDC2Tests(unittest.TestCase):
    def test_transform_pairs(self, *mocks):
        transformed_pairs = tuple(
            patch_dc2._transform_pairs(
                TEST_CALIB_OLD_ROOT, TEST_CALIB_NEW_ROOT, TEST_TEMPLATE
            )
        )

        self.assertEqual(transformed_pairs, TEST1_TRANSFORM_PAIRS)

    def test_transform_filter(self, *mocks):
        old_filter = "g"
        new_filter = "g_sim_1.4"
        returned_filter = patch_dc2._transform_filter_name(old_filter)
        self.assertEqual(returned_filter, new_filter)

    def test_copy_files(self, *mocks):
        patch_dc2.copy_files(TEST_CALIB_OLD_ROOT, TEST_CALIB_NEW_ROOT)
        mock_calls = mocks[0].mock_calls
        for mock_call, test_args in zip(mock_calls, TEST1_TRANSFORM_PAIRS):
            called_args = mock_call[1]
            self.assertEquals(called_args, test_args)

    @patch("lsst.dc2gen3.patch_dc2.copy_files", spec=True)
    def test_main(self, *mocks):
        exit_value = patch_dc2._main(
            [TEST_CALIB_OLD_ROOT, TEST_CALIB_NEW_ROOT]
        )
        self.assertEqual(exit_value, 0)
        copy_args = tuple(mocks[0].mock_calls[0])[1]
        self.assertEqual(copy_args, (TEST_CALIB_OLD_ROOT, TEST_CALIB_NEW_ROOT))


if __name__ == "__main__":
    unittest.main()
