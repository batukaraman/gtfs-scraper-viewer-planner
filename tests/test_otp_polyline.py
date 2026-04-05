"""Google polyline decode used for OTP legGeometry."""

import unittest

from planner.otp_client import decode_polyline


class TestOtpPolyline(unittest.TestCase):
    def test_decode_sample(self) -> None:
        # Short known-encoded path (lat,lng pairs)
        pts = decode_polyline("_p~iF~ps|U_ulLnnqC_mqNvxq`@")
        self.assertGreater(len(pts), 1)
        self.assertAlmostEqual(pts[0][0], 38.5, places=1)
        self.assertAlmostEqual(pts[0][1], -120.2, places=1)


if __name__ == "__main__":
    unittest.main()
