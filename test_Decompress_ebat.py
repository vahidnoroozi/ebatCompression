import unittest
import Decompress_ebat
from Decompress_ebat import CONST_TIMES_FLOATS


class TestPopulateFirstSetOfStats(unittest.TestCase):
    _DecodeFirstSetOfStats = Decompress_ebat.DecodeFirstSetOfStats()

    def test_populate_encoded_object_with_input(self):
        _input = ['USD.JPY\tEUR.JPY\t', '1', '  !', 'x\t', '1', '   ', 'A\t', '1', '   ', '0\t', '1', '   ',
                  '6311300000\t', '1', '   \t', '6311300770\t', '1', '   \t', '9791000\t12774000\t', '4',
                  '  :\\        ', '20\t0\t10\t', '1', '!" ']
        _object = TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.ObjTickerEncoded
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.populate_encoded_object_with_input(_object, _input)
        _list_of_unique_expected = ['USD.JPY', 'EUR.JPY']
        _int_byte_to_member_expected = 1
        _str_byte_encoded_expected = '  !'
        self.assertEquals(_object.list_unique_members, _list_of_unique_expected)
        self.assertEquals(_object.int_byte_to_member, _int_byte_to_member_expected)
        self.assertEquals(_object.list_byte_encoded, _str_byte_encoded_expected)

    def test_populate_time_objects(self):
        _object_time = TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.ObjTimeEncoded
        _object_time.list_unique_members = [1000,2000]
        _object_time.list_mapping_decoded = [[1,2,3],[4,5,7]]
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.populate_time_objects(_object_time)
        _final_decompressed_column_expected = [1001, 1002, 1003, 2004, 2005, 2007]
        self.assertEquals(_object_time.list_final_decompressed_column, _final_decompressed_column_expected)

    def test_parse_populate_byte_encoded(self):
        _object = TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.ObjTimeEncoded
        _object.list_byte_encoded = ' !; '
        _object.int_byte_to_member = 2
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.parse_populate_byte_encoded(_object)
        _list_parsed_byte_expected = [' !', '; ']
        _list_parsed_byte = _object.list_parsed_byte_encoded
        self.assertEquals(_list_parsed_byte, _list_parsed_byte_expected)

    def test_populate_dict_ascii_to_number(self):
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.populate_dict_ascii_to_number()
        _int_representation_of_space_character = TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.dict_map_ascii_to_number[' ']
        _int_representation_of_exclamation_point = TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.dict_map_ascii_to_number['!']
        _int_representation_of_space_character_expected = 0
        _int_representation_of_exclamation_point_expected = 1
        self.assertEquals(_int_representation_of_space_character, _int_representation_of_space_character_expected)
        self.assertEquals(_int_representation_of_exclamation_point, _int_representation_of_exclamation_point_expected)

    def test_convert_byte_to_integer_representation(self):
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.populate_dict_ascii_to_number()
        _int_representation_of_byte_to_integer = TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.convert_byte_to_integer_representation('! #')
        _int_representation_of_byte_to_integer_expected = 36103
        self.assertEquals(_int_representation_of_byte_to_integer, _int_representation_of_byte_to_integer_expected)

    def test_convert_list_of_encoded_bytes_to_list_of_integers(self):
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.populate_dict_ascii_to_number()
        _list_of_ints = TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.convert_list_of_encoded_bytes_to_list_of_integers(
            [' ', '! ', '#?'])
        _list_of_ints_expected = [0, 190, 601]
        self.assertEquals(_list_of_ints, _list_of_ints_expected)

    def test_populate_list_mapping_decoded(self):
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.populate_dict_ascii_to_number()
        _object_time = TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.ObjTickerEncoded
        _object_time.list_parsed_byte_encoded = [' ', '! ', '#?']
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.populate_list_mapping_decoded(_object_time)
        _list_mapping_decoded_expected = [0, 190, 601]
        self.assertEquals(_object_time.list_mapping_decoded, _list_mapping_decoded_expected)

    def test_populate_time_list_mapping_decoded(self):
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.populate_dict_ascii_to_number()
        _object = TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.ObjTimeEncoded
        _object.list_parsed_byte_encoded = [[' ', '! ', '#?'],[' ', '! ', '#?']]
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.populate_time_list_mapping_decoded(_object)
        _list_mapping_decoded_expected = [[0, 190, 601],[0, 190, 601]]
        self.assertEquals(_object.list_mapping_decoded, _list_mapping_decoded_expected)

    def test_populate_final_decompressed_column(self):
        _object = TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.ObjTickerEncoded
        _object.list_unique_members = ['AAA', 'BBB']
        _object.list_mapping_decoded = [0,1,1,0,1,1]
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.populate_final_decompressed_column(_object)
        _list_final_decompressed_column_expected = ['AAA', 'BBB', 'BBB', 'AAA', 'BBB', 'BBB']
        self.assertEquals(_object.list_final_decompressed_column, _list_final_decompressed_column_expected)

    def test_populate_dictionary_ticker_to_minimum_price(self):
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.ObjTickerEncoded.list_unique_members = ['AAA', 'BBB']
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.ObjPricesEncoded.list_unique_members = [1000, 2000]
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.populate_dictionary_ticker_to_minimum_price()
        _dict = TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.dict_ticker_to_minimum_price
        _expected_dict = {'AAA': 1000, 'BBB': 2000}
        self.assertDictEqual(_dict, _expected_dict)

    def test_populate_final_price_column(self):
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.ObjTickerEncoded.list_final_decompressed_column = ['AAA', 'BBB', 'BBB', 'AAA', 'BBB', 'BBB']
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.ObjPricesEncoded.list_mapping_decoded = [1000, 4000]
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.dict_ticker_to_minimum_price = {'AAA': 1000, 'BBB': 2000}
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.populate_final_price_column()
        _list_column = TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.ObjPricesEncoded.list_final_decompressed_column
        _list_column_expected = [2000/CONST_TIMES_FLOATS, 6000/CONST_TIMES_FLOATS]
        self.assertEquals(_list_column,_list_column_expected)

    def test_parse_populate_byte_time_encoded(self):
        _object = Decompress_ebat.EncodedObject(Decompress_ebat.int_time_row)
        _object.int_byte_to_member = 2
        _object.list_byte_encoded = '   \t   \t   '
        TestPopulateFirstSetOfStats._DecodeFirstSetOfStats.parse_populate_byte_time_encoded(_object)
        _list_parsed = _object.list_parsed_byte_encoded
        _list_parsed_expected = [['  ', ' '], ['  ', ' '], ['  ', ' ']]
        self.assertEquals(_list_parsed, _list_parsed_expected)


class TestDecodeSecondSetOfStats(unittest.TestCase):
    _DecodeFirstSetOfStats = Decompress_ebat.DecodeFirstSetOfStats()
    _DecodeSecondSetOfStats = Decompress_ebat.DecodeSecondSetOfStats(_DecodeFirstSetOfStats)

    def test_return_final_list(self):
        TestDecodeSecondSetOfStats._DecodeSecondSetOfStats.list_ticker = ['KMPR', 'CC']
        TestDecodeSecondSetOfStats._DecodeSecondSetOfStats.list_exchange= ['i', 'i']
        TestDecodeSecondSetOfStats._DecodeSecondSetOfStats.list_side = ['A', 'B']
        TestDecodeSecondSetOfStats._DecodeSecondSetOfStats.list_condition = ['0', '0']
        TestDecodeSecondSetOfStats._DecodeSecondSetOfStats.list_time = [20000,20003]
        TestDecodeSecondSetOfStats._DecodeSecondSetOfStats.list_rep_time = [20001, 20004]
        TestDecodeSecondSetOfStats._DecodeSecondSetOfStats.list_price = [97.23, 9333.11]
        TestDecodeSecondSetOfStats._DecodeSecondSetOfStats.list_size = [20, 3]
        TestDecodeSecondSetOfStats._DecodeSecondSetOfStats.return_final_list()
        _list_final = TestDecodeSecondSetOfStats._DecodeSecondSetOfStats.return_final_list()
        _list_final_expected = [['KMPR', 'i', 'A', '0', 20000, 20001, 97.23, 20],['CC', 'i', 'B', '0', 20003, 20004, 9333.11, 3]]
        self.assertEquals(_list_final, _list_final_expected)


if __name__ == "__main__":
    unittest.main()
