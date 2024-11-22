import unittest

from main import transform_row


class TestTransformRow(unittest.TestCase):
    def test_transform_row(self):
        # Input data
        row = {
            "startTime": "2024-11-22T10:00:00Z",
            "values": {
                "temperature": 21.5,
                "windSpeed": 5.2
            }
        }
        location = {
            "lat": 40.7128,
            "lon": -74.0060
        }

        # Expected output
        expected = {
            "latitude": 40.7128,
            "longitude": -74.0060,
            "snapshot_time": "2024-11-22T10:00:00Z",
            "temperature": 21.5,
            "wind_speed": 5.2,
        }

        # Assert the transformation is correct
        result = transform_row(row, location)
        self.assertEqual(result, expected)
