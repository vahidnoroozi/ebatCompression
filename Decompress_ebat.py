import re
import csv
import os

from Compress_ebat import CONST_UNIQUE_CHARACTERS_IN_A_BYTE
from Compress_ebat import CONST_TIMES_FLOATS
from Compress_ebat import int_ticker_column
from Compress_ebat import int_exchange_column
from Compress_ebat import int_side_column
from Compress_ebat import int_condition_column
from Compress_ebat import int_time_column
from Compress_ebat import int_rep_time_column
from Compress_ebat import int_price_column
from Compress_ebat import int_size_column

int_ticker_row = int_ticker_column
int_exchange_row = int_exchange_column
int_side_row = int_side_column
int_condition_row = int_condition_column
int_time_row = int_time_column
int_rep_time_row = int_rep_time_column
int_price_row = int_price_column
int_size_row = int_size_column


class FileReader(object):
    def __init__(self, path):
        self.path = path

    def return_lines(self):
        lines = [line.rstrip('\n') for line in open(self.path)]
        return lines

    def save_list_to_csv(self, list_final):
        str_name_of_file = "{0}_decompressed.csv".format(os.path.splitext(self.path)[0])
        with open(str_name_of_file, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(list_final)


class EncodedObject:
    def __init__(self, int_row):
        self.list_unique_members = []
        self.int_byte_to_member = 0
        self.list_byte_encoded = []
        self.list_parsed_byte_encoded = []
        self.list_mapping_decoded = []
        self.list_final_decompressed_column = []
        self.int_row = int_row


class DecodeFirstSetOfStats:
    def __init__(self):
        self.ObjTickerEncoded = EncodedObject(int_ticker_row)
        self.ObjExchangeEncoded = EncodedObject(int_exchange_row)
        self.ObjSideEncoded = EncodedObject(int_side_row)
        self.ObjConditionEncoded = EncodedObject(int_condition_row)
        self.ObjTimeEncoded = EncodedObject(int_time_row)
        self.ObjRepTimeEncoded = EncodedObject(int_rep_time_row)
        self.ObjPricesEncoded = EncodedObject(int_price_row)
        self.ObjSizeEncoded = EncodedObject(int_size_row)
        self.dict_map_ascii_to_number = {}
        self.dict_ticker_to_minimum_price = {}

    @staticmethod
    def populate_encoded_object_with_input(object_encoded: EncodedObject, list_received_input):
        _int_obj_row = object_encoded.int_row
        _list_of_unique_members_plus_tab = re.split(r'\t+', list_received_input[_int_obj_row * 3])
        object_encoded.list_unique_members = _list_of_unique_members_plus_tab[:-1]
        object_encoded.int_byte_to_member = int(list_received_input[_int_obj_row * 3 + 1])
        object_encoded.list_byte_encoded = list_received_input[_int_obj_row * 3 + 2]

    @staticmethod
    def populate_time_objects(obj_time_info_encoded: EncodedObject):
        _list_of_time_refs = obj_time_info_encoded.list_unique_members
        _list_of_list_of_times = obj_time_info_encoded.list_mapping_decoded
        for _int_index in range(len(_list_of_time_refs)):

            _list_new_times = _list_of_list_of_times[_int_index]
            _int_time_ref = int(_list_of_time_refs[_int_index])
            for _int_time in _list_new_times:
                _int_final_time_value = int(_int_time) + _int_time_ref
                obj_time_info_encoded.list_final_decompressed_column.append(_int_final_time_value)

    @staticmethod
    def parse_populate_byte_encoded(object_encoded: EncodedObject):
        _int_bytes_each = object_encoded.int_byte_to_member
        _str_encoded_string = object_encoded.list_byte_encoded
        object_encoded.list_parsed_byte_encoded = [_str_encoded_string[i:i + _int_bytes_each] for i in
                                                   range(0, len(_str_encoded_string), _int_bytes_each)]

    def populate_dict_ascii_to_number(self):
        for int_number in range(256):
            if int_number <= 94:
                self.dict_map_ascii_to_number[chr(int_number + 32)] = int_number
            elif 95 <= int_number < 190:
                self.dict_map_ascii_to_number[chr(int_number + 66)] = int_number

    def convert_byte_to_integer_representation(self, _str_input):
        _int_value = 0
        _list_input = str(_str_input)
        _length_input = len(_list_input)
        for _int_index in range(_length_input):
            _chr_byte = _list_input[-(_int_index + 1)]
            _int_value += self.dict_map_ascii_to_number[_chr_byte] * pow(CONST_UNIQUE_CHARACTERS_IN_A_BYTE, _int_index)
        return _int_value

    def convert_list_of_encoded_bytes_to_list_of_integers(self, _list_bytes):
        _list_int_out = []
        for _str_item in _list_bytes:
            _int_value = self.convert_byte_to_integer_representation(_str_item)
            _list_int_out.append(_int_value)
        return _list_int_out

    def populate_list_mapping_decoded(self, object_encoded: EncodedObject):
        _list_of_ascii_endoded = object_encoded.list_parsed_byte_encoded
        _list_of_integers = self.convert_list_of_encoded_bytes_to_list_of_integers(_list_of_ascii_endoded)
        object_encoded.list_mapping_decoded = _list_of_integers

    def populate_time_list_mapping_decoded(self, object_encoded: EncodedObject):
        _list_of_ascii_endoded = object_encoded.list_parsed_byte_encoded
        _list_nested_list_of_integers = []
        for _list in _list_of_ascii_endoded:
            _list_of_integers = self.convert_list_of_encoded_bytes_to_list_of_integers(_list)
            _list_nested_list_of_integers.append(_list_of_integers)
        object_encoded.list_mapping_decoded = _list_nested_list_of_integers

    @staticmethod
    def populate_final_decompressed_column(object_encoded: EncodedObject):
        _list_of_unique = object_encoded.list_unique_members
        _list_of_mappings = object_encoded.list_mapping_decoded
        for _int_mapping in _list_of_mappings:
            _str_key = _list_of_unique[_int_mapping]
            object_encoded.list_final_decompressed_column.append(_str_key)

    def populate_dictionary_ticker_to_minimum_price(self):
        _int_length_tickers_list = len(self.ObjTickerEncoded.list_unique_members)
        for _int_index in range(_int_length_tickers_list):
            _str_ticker = self.ObjTickerEncoded.list_unique_members[_int_index]
            _minimum_price = self.ObjPricesEncoded.list_unique_members[_int_index]
            self.dict_ticker_to_minimum_price[_str_ticker] = _minimum_price

    def populate_final_price_column(self):
        _list_of_mappings = self.ObjPricesEncoded.list_mapping_decoded
        for _int_index in range(len(_list_of_mappings)):
            _str_ticker = self.ObjTickerEncoded.list_final_decompressed_column[_int_index]
            _float_relative_price = float(self.ObjPricesEncoded.list_mapping_decoded[_int_index])
            _float_price = float(self.dict_ticker_to_minimum_price[_str_ticker]) + _float_relative_price
            _float_price /= CONST_TIMES_FLOATS
            self.ObjPricesEncoded.list_final_decompressed_column.append(_float_price)

    @staticmethod
    def parse_populate_byte_time_encoded(object_encoded: EncodedObject):
        _int_bytes_each = object_encoded.int_byte_to_member
        _str_encoded_string = str(object_encoded.list_byte_encoded)
        _list_of_lists_time = re.split(r'\t+', _str_encoded_string)
        for _str_list in _list_of_lists_time:
            _list_sub_list = [_str_list[i:i + _int_bytes_each] for i in range(0, len(_str_list), _int_bytes_each)]
            object_encoded.list_parsed_byte_encoded.append(_list_sub_list)


class DecodeSecondSetOfStats(DecodeFirstSetOfStats):
    def __init__(self, obj_populated: DecodeFirstSetOfStats):
        super().__init__()
        self.list_ticker = obj_populated.ObjTickerEncoded.list_final_decompressed_column
        self.list_exchange = obj_populated.ObjExchangeEncoded.list_final_decompressed_column
        self.list_side = obj_populated.ObjSideEncoded.list_final_decompressed_column
        self.list_condition = obj_populated.ObjConditionEncoded.list_final_decompressed_column
        self.list_time = obj_populated.ObjTimeEncoded.list_final_decompressed_column
        self.list_rep_time = obj_populated.ObjRepTimeEncoded.list_final_decompressed_column
        self.list_price = obj_populated.ObjPricesEncoded.list_final_decompressed_column
        self.list_size = obj_populated.ObjSizeEncoded.list_final_decompressed_column

    def return_final_list(self):
        final_list = list()
        _int_length = len(self.list_ticker)
        for _int_index in range(_int_length):
            _list = [self.list_ticker[_int_index], self.list_exchange[_int_index], self.list_side[_int_index],
                     self.list_condition[_int_index], self.list_time[_int_index], self.list_rep_time[_int_index],
                     self.list_price[_int_index], self.list_size[_int_index]]
            final_list.append(_list)
        return final_list


def main():
    file_path = input("Enter the complete path to the compressed ebat file, including the file name and its extension:\n")
    #file_path = "C:\\Users\\Vahid\\PycharmProjects\\QuantlabCompression\\QuantlabCompressionPackage\\ebat.QuantLabcompressionExercise"
    readwrite = FileReader(file_path)
    list_lines = readwrite.return_lines()

    object_first_processor = DecodeFirstSetOfStats()

    object_first_processor.populate_encoded_object_with_input(object_first_processor.ObjTickerEncoded, list_lines)
    object_first_processor.populate_encoded_object_with_input(object_first_processor.ObjExchangeEncoded, list_lines)
    object_first_processor.populate_encoded_object_with_input(object_first_processor.ObjSideEncoded, list_lines)
    object_first_processor.populate_encoded_object_with_input(object_first_processor.ObjConditionEncoded, list_lines)
    object_first_processor.populate_encoded_object_with_input(object_first_processor.ObjTimeEncoded, list_lines)
    object_first_processor.populate_encoded_object_with_input(object_first_processor.ObjRepTimeEncoded, list_lines)
    object_first_processor.populate_encoded_object_with_input(object_first_processor.ObjPricesEncoded, list_lines)
    object_first_processor.populate_encoded_object_with_input(object_first_processor.ObjSizeEncoded, list_lines)

    object_first_processor.parse_populate_byte_encoded(object_first_processor.ObjTickerEncoded)
    object_first_processor.parse_populate_byte_encoded(object_first_processor.ObjExchangeEncoded)
    object_first_processor.parse_populate_byte_encoded(object_first_processor.ObjSideEncoded)
    object_first_processor.parse_populate_byte_encoded(object_first_processor.ObjConditionEncoded)
    object_first_processor.parse_populate_byte_encoded(object_first_processor.ObjPricesEncoded)
    object_first_processor.parse_populate_byte_encoded(object_first_processor.ObjSizeEncoded)

    object_first_processor.parse_populate_byte_time_encoded(object_first_processor.ObjTimeEncoded)
    object_first_processor.parse_populate_byte_time_encoded(object_first_processor.ObjRepTimeEncoded)

    object_first_processor.populate_dict_ascii_to_number()

    object_first_processor.populate_list_mapping_decoded(object_first_processor.ObjTickerEncoded)
    object_first_processor.populate_list_mapping_decoded(object_first_processor.ObjExchangeEncoded)
    object_first_processor.populate_list_mapping_decoded(object_first_processor.ObjSideEncoded)
    object_first_processor.populate_list_mapping_decoded(object_first_processor.ObjConditionEncoded)
    object_first_processor.populate_list_mapping_decoded(object_first_processor.ObjPricesEncoded)
    object_first_processor.populate_list_mapping_decoded(object_first_processor.ObjSizeEncoded)

    object_first_processor.populate_time_list_mapping_decoded(object_first_processor.ObjTimeEncoded)
    object_first_processor.populate_time_list_mapping_decoded(object_first_processor.ObjRepTimeEncoded)

    object_first_processor.populate_final_decompressed_column(object_first_processor.ObjTickerEncoded)
    object_first_processor.populate_final_decompressed_column(object_first_processor.ObjExchangeEncoded)
    object_first_processor.populate_final_decompressed_column(object_first_processor.ObjSideEncoded)
    object_first_processor.populate_final_decompressed_column(object_first_processor.ObjConditionEncoded)
    object_first_processor.populate_final_decompressed_column(object_first_processor.ObjSizeEncoded)

    object_first_processor.populate_dictionary_ticker_to_minimum_price()

    object_first_processor.populate_final_price_column()
    object_first_processor.populate_time_objects(object_first_processor.ObjTimeEncoded)
    object_first_processor.populate_time_objects(object_first_processor.ObjRepTimeEncoded)

    object_second_processor = DecodeSecondSetOfStats(object_first_processor)

    final_list = object_second_processor.return_final_list()

    file_writer = FileReader(file_path)
    file_writer.save_list_to_csv(final_list)


if __name__ == '__main__':
    main()
