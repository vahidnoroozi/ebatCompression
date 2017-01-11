import unittest
import Compress_ebat
from Compress_ebat import CONST_TIMES_FLOATS


class TestGetFirstSetOfStats(unittest.TestCase):
    _GetFirstSetOfStatsInstance = Compress_ebat.GetFirstSetOfStats()

    def test_update_dict_unique_tickers_to_minimum_price(self):
        TestGetFirstSetOfStats._GetFirstSetOfStatsInstance.dict_tickers_to_minimum_price = {}
        _list_input1 = ['CLJ8', 'F', 'T', '0', '3358103', '3358103', '95.53', '1']
        _list_input2 = ['CLJ8', 'F', 'T', '0', '3358104', '3358104', '95.51', '1']
        TestGetFirstSetOfStats._GetFirstSetOfStatsInstance.update_dict_unique_tickers_to_minimum_price(_list_input1)
        TestGetFirstSetOfStats._GetFirstSetOfStatsInstance.update_dict_unique_tickers_to_minimum_price(_list_input2)
        _dict_ticker_to_minimum_price = TestGetFirstSetOfStats._GetFirstSetOfStatsInstance.dict_tickers_to_minimum_price
        _dict_ticker_to_minimum_price_expected = {'CLJ8': 95.51*CONST_TIMES_FLOATS}
        self.assertDictEqual(_dict_ticker_to_minimum_price, _dict_ticker_to_minimum_price_expected)

    def test_get_time_gaps(self):
        _object_time = Compress_ebat.EncodeTimeObject(Compress_ebat.int_time_column)
        _list_input1 = ['CLJ8','F','T','0','3358103','3358103','95.53','1']
        _list_input2 = ['CLJ8','F','T','0','3358104','3358104','95.51','1']
        TestGetFirstSetOfStats._GetFirstSetOfStatsInstance.get_time_gaps(_object_time,_list_input1)
        TestGetFirstSetOfStats._GetFirstSetOfStatsInstance.get_time_gaps(_object_time,_list_input2)
        _list_time_gaps = _object_time.list_time_gap
        _int_last_time = _object_time.int_last_time
        _list_ref_time = _object_time.list_time_references
        _int_last_time_expected = 3358104
        _list_time_gaps_expected = [[0,1]]
        _list_time_reference_expected = [3358103]
        self.assertEquals(_int_last_time, _int_last_time_expected)
        self.assertEquals(_list_time_gaps, _list_time_gaps_expected)
        self.assertEquals(_list_ref_time, _list_time_reference_expected)

    def test_add_to_list(self):
        _object = Compress_ebat.EncodeObject(Compress_ebat.int_side_column)
        _input1 = ['CLJ8', 'F', 'T', '0', '3358103', '3358103', '95.53', '1']
        TestGetFirstSetOfStats._GetFirstSetOfStatsInstance.add_to_list(_object, _input1)
        _list_of_items_expected = ['T']
        self.assertEquals(_object.list_of_items, _list_of_items_expected)


class TestGetSecondSetOfStats(unittest.TestCase):
    _GetFirstSetOfStatsInstance = Compress_ebat.GetFirstSetOfStats()
    _GetSecondSetOfStatsInstance = Compress_ebat.GetSecondSetOfStats(_GetFirstSetOfStatsInstance)

    def test_populate_lists_of_unique_tickers_and_minimum_price(self):
        TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.ObjRelativePriceFIRSTEncode = Compress_ebat.EncodeObject(Compress_ebat.int_price_column)
        TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.dict_tickers_to_minimum_price = {'A':2, 'B':4}
        TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.populate_lists_of_unique_tickers_and_minimum_price()
        _list_of_tickers_unique = TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.ObjTickerFIRSTEncode.list_unique_members
        _list_of_minimum_price = TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.ObjRelativePriceFIRSTEncode.list_unique_members
        _list_of_tickers_unique_expected = ['A', 'B']
        _list_of_minimum_price_expected = [2, 4]
        self.assertEquals(set(_list_of_minimum_price), set(_list_of_minimum_price_expected))
        self.assertCountEqual(_list_of_minimum_price,_list_of_minimum_price_expected)
        self.assertEquals(set(_list_of_tickers_unique), set(_list_of_tickers_unique_expected))
        self.assertCountEqual(_list_of_tickers_unique, _list_of_tickers_unique_expected)

    def test_populate_dict_item_to_index(self):
        _object = Compress_ebat.EncodeObject(Compress_ebat.int_ticker_column)
        _object.list_unique_members = ['A','B']
        TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.populate_dict_item_to_index(_object)
        _dict_item_to_index_expected = {'A': 0, 'B': 1}
        self.assertDictEqual(_object.dict_member_to_index, _dict_item_to_index_expected)

    def test_length_of_number_in_another_base(self):
        _int_number = 1000
        _int_base = 200
        _int_length = TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.length_of_number_in_another_base(_int_number,_int_base)
        _int_length_expected = 2
        self.assertEquals(_int_length, _int_length_expected)

    def test_how_many_bytes_required(self):
        _number = 1000
        _int_length = TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.how_many_bytes_required(_number)
        _int_length_expected = 2
        self.assertEquals(_int_length, _int_length_expected)

    def test_perform_first_encoding_of_object(self):
        _object = Compress_ebat.EncodeObject(1)
        _object.list_of_items = ['A', 'B', 'C','D', 'D', 'D']
        TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.perform_first_encoding_of_object(_object)
        _list_unique_list = _object.list_unique_members
        _int_byte_to_member = _object.int_byte_to_member
        _list_unique_list_expected = ['A', 'B', 'C','D']
        _int_byte_to_member_obj_expected = 1
        self.assertSetEqual(set(_list_unique_list), set(_list_unique_list_expected))
        self.assertCountEqual(_list_unique_list, _list_unique_list_expected)
        self.assertEquals(_int_byte_to_member, _int_byte_to_member_obj_expected)

    def test_perform_second_encoding_of_object(self):
        _input1 = ['CLJ8', 'F', 'T', '0', '3358103', '3358103', '95.53', '1']
        _object = Compress_ebat.EncodeObject(Compress_ebat.int_ticker_column)
        _object.dict_member_to_index['CLJ8'] = 6
        TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.perform_second_encoding_of_object(_object, _input1)
        _list_mapping = _object.list_mappings
        _list_mapping_expected = [6]
        self.assertEquals(_list_mapping, _list_mapping_expected)

    def test_perform_first_encoding_of_time_object(self):
        _object = Compress_ebat.EncodeTimeObject(Compress_ebat.int_time_column)
        _object.list_time_gap = [[1,2,3,4,5,6,7],[2333,1,5]]
        _object.list_time_references = [1,3000]
        TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.perform_first_encoding_of_time_object(_object)
        _list_map_expected = [[1,2,3,4,5,6,7],[2333,1,5]]
        _list_unique_expected = _object.list_time_references
        _byte_to_member_expected = 2
        self.assertEquals(_object.list_mappings, _list_map_expected)
        self.assertEquals(_object.list_unique_members, _list_unique_expected, )
        self.assertEquals(_object.int_byte_to_member, _byte_to_member_expected)

    def test_perform_first_encoding_of_price_object(self):
        TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.ObjRelativePriceFIRSTEncode.list_unique_members = [100,99,97,1021]
        TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.perform_first_encoding_of_price_object()
        _int_byte_to_member_expected = 2
        _int_byte_to_member = TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.ObjRelativePriceFIRSTEncode.int_byte_to_member
        self.assertEquals(_int_byte_to_member_expected, _int_byte_to_member)

    def test_perform_second_encoding_of_price_object(self):
        _input1 = ['CLJ8', 'F', 'T', '0', '3358103', '3358103', '95.53', '1']
        TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.dict_tickers_to_minimum_price = {'CLJ8': int(95.63*CONST_TIMES_FLOATS)}
        TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.perform_second_encoding_of_price_object(_input1)
        _list_mapping_expected = [-int(0.1*CONST_TIMES_FLOATS)]
        _list_mapping = TestGetSecondSetOfStats._GetSecondSetOfStatsInstance.ObjRelativePriceFIRSTEncode.list_mappings
        self.assertEquals(_list_mapping, _list_mapping_expected)


class TestGetThirdSetOfStats(unittest.TestCase):
    _GetFirstSetStatsObject = Compress_ebat.GetFirstSetOfStats()
    _GetSecondaryStatsObject = Compress_ebat.GetSecondSetOfStats(_GetFirstSetStatsObject)
    _GetThirdSetStatsObject = Compress_ebat.GetThirdSetOfStats(_GetSecondaryStatsObject, _GetSecondaryStatsObject)

    def test_populate_dict_map_number_to_ascii(self):
        TestGetThirdSetOfStats._GetThirdSetStatsObject.populate_dict_map_number_to_ascii()
        _char_of_one = TestGetThirdSetOfStats._GetThirdSetStatsObject.dict_map_number_to_ascii[1]
        _char_of_hundred = TestGetThirdSetOfStats._GetThirdSetStatsObject.dict_map_number_to_ascii[100]
        _char_of_one_expected = '!'
        _char_of_hundred_expected = '¦'
        self.assertEquals(_char_of_one, _char_of_one_expected)
        self.assertEquals(_char_of_hundred, _char_of_hundred_expected)

    def test_convert_to_byte_representation(self):
        TestGetThirdSetOfStats._GetThirdSetStatsObject.populate_dict_map_number_to_ascii()
        _str_byte_representation = TestGetThirdSetOfStats._GetThirdSetStatsObject.convert_to_byte_representation(533, 3)
        _str_byte_representation_expected = ' "Û'
        self.assertEquals(_str_byte_representation, _str_byte_representation_expected)

    def test_populate_list_byte_encoded(self):
        _object = TestGetThirdSetOfStats._GetThirdSetStatsObject.ObjTickerFIRSTEncode
        TestGetThirdSetOfStats._GetThirdSetStatsObject.populate_dict_map_number_to_ascii()
        _object.list_mappings = [1,2,3,400]
        _object.int_byte_to_member = 2
        TestGetThirdSetOfStats._GetThirdSetStatsObject.populate_list_byte_encoded(_object)
        _encoded_list = _object.list_byte_encoded
        _encoded_list_expected = [' !', ' "', ' #', '"4']
        self.assertEquals(_encoded_list, _encoded_list_expected)

    def test_populate_list_time_byte_encoded(self):
        _object = TestGetThirdSetOfStats._GetThirdSetStatsObject.ObjTimeFIRSTEncode
        TestGetThirdSetOfStats._GetThirdSetStatsObject.populate_dict_map_number_to_ascii()
        _object.list_mappings = [[1, 2, 3, 400], [2, 4, 55, 60], [0, 1]]
        _object.int_byte_to_member = 2
        TestGetThirdSetOfStats._GetThirdSetStatsObject.populate_list_time_byte_encoded(_object)
        _encoded_list = _object.list_byte_encoded
        _encoded_list_expected = [[' !', ' "', ' #', '"4'], [' "', ' $', ' W', ' \\'], ['  ', ' !']]
        self.assertEquals(_encoded_list, _encoded_list_expected)

    def test_put_obj_list_in_output_list(self):
        _object = TestGetThirdSetOfStats._GetThirdSetStatsObject.ObjTickerFIRSTEncode
        _object.list_byte_encoded = [' !', ' "', ' #', '"4']
        _object.list_unique_members = ['A', 'B']
        _object.int_byte_to_member = 2
        TestGetThirdSetOfStats._GetThirdSetStatsObject.put_obj_list_in_output_list(_object)
        _list_output = TestGetThirdSetOfStats._GetThirdSetStatsObject.list_outgoing
        _list_output_expected = ['A', '\t', 'B', '\t', '\n', 2, '\n', ' !', ' "', ' #', '"4', '\n']
        self.assertEquals(_list_output, _list_output_expected)

    def test_put_time_obj_list_in_output_list(self):
        _object_time = TestGetThirdSetOfStats._GetThirdSetStatsObject.ObjTimeFIRSTEncode
        TestGetThirdSetOfStats._GetThirdSetStatsObject.list_outgoing = []
        _object_time.list_byte_encoded = [[' !', '"4'], [' "', ' $']]
        _object_time.list_unique_members = [100, 2000]
        _object_time.int_byte_to_member = 2
        TestGetThirdSetOfStats._GetThirdSetStatsObject.put_time_obj_list_in_output_list(_object_time)
        _list_output = TestGetThirdSetOfStats._GetThirdSetStatsObject.list_outgoing
        _list_output_expected = [100, '\t', 2000, '\t', '\n', 2, '\n', ' !', '"4', '\t', ' "', ' $', '\t', '\n']
        self.assertEquals(_list_output, _list_output_expected)

if __name__ == "__main__":
    unittest.main()