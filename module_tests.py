import math
import unittest
from vwap_module import calculate_vwap


class TestVWAP(unittest.TestCase):

    def test_vwap_correctly_calculated(self):
        "Simulating a 3 point window for calculating vwap"

        values_list = [{'price': 100, 'size': 10}, {'price': 200, 'size': 100}, {'price': 150, 'size': 20}, {'price': 300, 'size': 30}]
        numerator, denominator, vwap = calculate_vwap(0, 0, values_list[0])
        assert vwap == 100

        numerator, denominator, vwap = calculate_vwap(numerator, denominator, values_list[1])

        assert math.isclose(vwap, 190.909090, abs_tol=0.000001)

        numerator, denominator, vwap = calculate_vwap(numerator, denominator, values_list[2])

        assert math.isclose(vwap, 184.615384615, abs_tol=0.000001)

        "Dropping index 0 of values_list in favour of index 3"
        numerator, denominator, vwap = calculate_vwap(numerator, denominator, values_list[3], values_list[0])

        assert math.isclose(vwap, 213.333333333, abs_tol=0.000001)


if __name__ == '__main__':
    unittest.main()