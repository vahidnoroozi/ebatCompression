import csv
import math
import os

CONST_UNIQUE_CHARACTERS_IN_A_BYTE = int(255-160 + 126 - 31)  # Not using 0 to 32 and the codes 127 to 160
CONST_NUMBER_OF_FLOATING_POINTS_IN_USD = 5  #The default number of decimal digits used for precision of USD price column
CONST_TIMES_FLOATS = pow(10,CONST_NUMBER_OF_FLOATING_POINTS_IN_USD)
CONST_INT_MAX_TIME_GAP_ALLOWED = int(math.pow(CONST_UNIQUE_CHARACTERS_IN_A_BYTE, 2) - 1)
CONST_DICT_EBAT_FILE_STRUCTURE = {'ticker': 0,
                                  'exchange': 1,
                                  'side': 2,
                                  'condition': 3,
                                  'time': 4,
                                  'rep_time': 5,
                                  'price': 6,
                                  'size': 7}
int_ticker_column = CONST_DICT_EBAT_FILE_STRUCTURE['ticker']
int_exchange_column = CONST_DICT_EBAT_FILE_STRUCTURE['exchange']
int_side_column = CONST_DICT_EBAT_FILE_STRUCTURE['side']
int_condition_column = CONST_DICT_EBAT_FILE_STRUCTURE['condition']
int_time_column = CONST_DICT_EBAT_FILE_STRUCTURE['time']
int_rep_time_column = CONST_DICT_EBAT_FILE_STRUCTURE['rep_time']
int_price_column = CONST_DICT_EBAT_FILE_STRUCTURE['price']
int_size_column = CONST_DICT_EBAT_FILE_STRUCTURE['size']


class CSVReader(object):
    def __init__(self, path):
        self.path = path

    def __iter__(self):
        with open(self.path, 'rU') as data:
            reader = csv.reader(data)
            for row in reader:
                yield row


class EncodeObject:
    def __init__(self, int_column):
        self.list_unique_members = []
        self.int_byte_to_member = 0
        self.list_mappings = []
        self.list_byte_encoded = []
        self.int_column = int_column
        self.list_of_items = []
        self.dict_member_to_index = {}


class EncodeTimeObject(EncodeObject):
    def __init__(self, int_column):
        super().__init__(int_column)
        self.int_last_time = 0  # Initialized
        self.list_time_references = []
        self.list_time_gap = [[]]
        self.int_column = int_column


class GetFirstSetOfStats(object):
    def __init__(self):
        self.dict_tickers_to_minimum_price = {}
        self.ObjTickerFIRSTEncode = EncodeObject(int_ticker_column)
        self.ObjExchangeFIRSTEncode = EncodeObject(int_exchange_column)
        self.ObjSideFIRSTEncode = EncodeObject(int_side_column)
        self.ObjConditionFIRSTEncode = EncodeObject(int_condition_column)
        self.ObjRelativePriceFIRSTEncode = EncodeObject(int_price_column)
        self.ObjSizeFIRSTEncode = EncodeObject(int_size_column)

        self.ObjTimeFIRSTEncode = EncodeTimeObject(int_time_column)
        self.ObjRepTimeFIRSTEncode = EncodeTimeObject(int_rep_time_column)

    def update_dict_unique_tickers_to_minimum_price(self, list_new_input_row: list):
        _str_ticker = list_new_input_row[int_ticker_column]
        _int_price = int(float(list_new_input_row[int_price_column]) * CONST_TIMES_FLOATS)

        if _str_ticker not in self.dict_tickers_to_minimum_price:
            self.dict_tickers_to_minimum_price[_str_ticker] = _int_price
        else:
            _current_price = self.dict_tickers_to_minimum_price[_str_ticker]
            self.dict_tickers_to_minimum_price[_str_ticker] = min(_current_price, _int_price)

    @staticmethod
    def get_time_gaps(obj_time_object: EncodeTimeObject, list_new_input_row: list):
        _int_new_time = int(list_new_input_row[obj_time_object.int_column])
        if len(obj_time_object.list_time_references) == 0:
            obj_time_object.list_time_references.append(_int_new_time)
        if len(obj_time_object.list_time_gap) == 0:
            obj_time_object.list_time_gap.append([])
        if 0 <= _int_new_time - obj_time_object.list_time_references[-1] < CONST_INT_MAX_TIME_GAP_ALLOWED:
            obj_time_object.list_time_gap[-1].append(_int_new_time - obj_time_object.list_time_references[-1])
        else:
            obj_time_object.list_time_references.append(_int_new_time)
            obj_time_object.list_time_gap.append([])
            obj_time_object.list_time_gap[-1].append(_int_new_time - obj_time_object.list_time_references[-1])
        obj_time_object.int_last_time = _int_new_time

    @staticmethod
    def add_to_list(obj_target: EncodeObject, list_new_input_row:list):
        _str_new_item = list_new_input_row[obj_target.int_column]
        obj_target.list_of_items.append(_str_new_item)


class GetSecondSetOfStats(GetFirstSetOfStats):
    def __init__(self, obj_first_set_of_stats: GetFirstSetOfStats):
        super().__init__()
        self.dict_tickers_to_minimum_price = obj_first_set_of_stats.dict_tickers_to_minimum_price
        self.ObjTickerFIRSTEncode = obj_first_set_of_stats.ObjTickerFIRSTEncode
        self.ObjExchangeFIRSTEncode = obj_first_set_of_stats.ObjExchangeFIRSTEncode
        self.ObjSideFIRSTEncode = obj_first_set_of_stats.ObjSideFIRSTEncode
        self.ObjConditionFIRSTEncode = obj_first_set_of_stats.ObjConditionFIRSTEncode
        self.ObjRelativePriceFIRSTEncode = obj_first_set_of_stats.ObjRelativePriceFIRSTEncode
        self.ObjSizeFIRSTEncode = obj_first_set_of_stats.ObjSizeFIRSTEncode

        self.ObjTimeFIRSTEncode = obj_first_set_of_stats.ObjTimeFIRSTEncode
        self.ObjRepTimeFIRSTEncode = obj_first_set_of_stats.ObjRepTimeFIRSTEncode

    def populate_lists_of_unique_tickers_and_minimum_price(self):
        for _str_key in self.dict_tickers_to_minimum_price:
            self.ObjTickerFIRSTEncode.list_unique_members.append(_str_key)
            self.ObjRelativePriceFIRSTEncode.list_unique_members.append(self.dict_tickers_to_minimum_price[_str_key])

    @staticmethod
    def populate_dict_item_to_index(obj_target: EncodeObject):
        _int_index = 0
        for _str_item in obj_target.list_unique_members:
            obj_target.dict_member_to_index[_str_item] = _int_index
            _int_index += 1

    @staticmethod
    def length_of_number_in_another_base(num: int, base: int):
        if num < 0 or type(num) != int:
            raise ValueError('input number should be a non-negative integer')
        elif base < 0 or type(base) != int:
            raise ValueError('base should be a non-negative integer')
        _num = num
        _int_length = 1
        try:
            while int(_num / base) > 0:
                _num = int(_num / base)
                _int_length += 1
        except ZeroDivisionError:
            print("Base cannot be zero")
        return _int_length

    def how_many_bytes_required(self, int_input: int) -> int:
        _int_input = int_input
        if _int_input >= CONST_UNIQUE_CHARACTERS_IN_A_BYTE:
            _int_required_bytes = self.length_of_number_in_another_base(_int_input,
                                                                        CONST_UNIQUE_CHARACTERS_IN_A_BYTE)
        else:
            # TODO use this in the future to improve the compression
            _int_required_bytes = int(1 / (
                self.length_of_number_in_another_base(_int_input, CONST_UNIQUE_CHARACTERS_IN_A_BYTE)))
        return _int_required_bytes

    def perform_first_encoding_of_object(self, object_target: EncodeObject):
        if len(object_target.list_unique_members) == 0:
            _list_unique_members = list(set(object_target.list_of_items))
            object_target.list_unique_members = _list_unique_members
        _number_of_bytes_required = self.how_many_bytes_required(len(object_target.list_unique_members))
        object_target.int_byte_to_member = _number_of_bytes_required
        # TODO use this in the future to improve the compression
        if _number_of_bytes_required < 1:
            pass

    @staticmethod
    def perform_second_encoding_of_object(obj_target: EncodeObject, list_new_input_row: list):
        _str_new_item = list_new_input_row[obj_target.int_column]
        assert isinstance(_str_new_item, str)
        _int_item_index = obj_target.dict_member_to_index[_str_new_item]
        obj_target.list_mappings.append(_int_item_index)

    def perform_first_encoding_of_time_object(self, obj_target: EncodeTimeObject):
        _number_of_bytes_required = self.how_many_bytes_required(max(max(obj_target.list_time_gap)))
        # TODO use this in the future to improve the compression
        if _number_of_bytes_required < 1:
            pass
        obj_target.int_byte_to_member = _number_of_bytes_required
        obj_target.list_unique_members = obj_target.list_time_references
        obj_target.list_mappings = obj_target.list_time_gap

    def perform_first_encoding_of_price_object(self):
        _number_of_bytes_required = self.how_many_bytes_required(max(self.ObjRelativePriceFIRSTEncode.list_unique_members))
        # TODO use this in the future to improve the compression
        if _number_of_bytes_required < 1:
            pass
        self.ObjRelativePriceFIRSTEncode.int_byte_to_member = _number_of_bytes_required

    def perform_second_encoding_of_price_object(self, list_new_input_row):
        _int_new_price = int(float(list_new_input_row[int_price_column]) * CONST_TIMES_FLOATS)
        _str_new_ticker = list_new_input_row[int_ticker_column]
        _int_price_diff_to_minimum_of_same_ticker = _int_new_price - self.dict_tickers_to_minimum_price[
            _str_new_ticker]
        # assert isinstance(_str_new_size, str)
        self.ObjRelativePriceFIRSTEncode.list_mappings.append(_int_price_diff_to_minimum_of_same_ticker)


class GetThirdSetOfStats(GetSecondSetOfStats):
    def __init__(self, obj_secondary_stats: GetSecondSetOfStats, obj_first_set_of_stats: GetFirstSetOfStats):
        super().__init__(obj_first_set_of_stats)
        self.ObjTickerFIRSTEncode = obj_secondary_stats.ObjTickerFIRSTEncode
        self.ObjExchangeFIRSTEncode = obj_secondary_stats.ObjExchangeFIRSTEncode
        self.ObjSideFIRSTEncode = obj_secondary_stats.ObjSideFIRSTEncode
        self.ObjConditionFIRSTEncode = obj_secondary_stats.ObjConditionFIRSTEncode
        self.ObjTimeFIRSTEncode = obj_secondary_stats.ObjTimeFIRSTEncode
        self.ObjRepTimeFIRSTEncode = obj_secondary_stats.ObjRepTimeFIRSTEncode
        self.ObjRelativePriceFIRSTEncode = obj_secondary_stats.ObjRelativePriceFIRSTEncode
        self.ObjSizeFIRSTEncode = obj_secondary_stats.ObjSizeFIRSTEncode
        self.dict_map_number_to_ascii = {}
        self.list_outgoing = []

    def populate_dict_map_number_to_ascii(self):
        for int_number in range(256):
            if int_number <= 94:
                self.dict_map_number_to_ascii[int_number] = chr(int_number + 32)
            elif 95 <= int_number < 190:
                self.dict_map_number_to_ascii[int_number] = chr(int_number + 66)

    def convert_to_byte_representation(self, int_value: int, int_number_of_bytes: int):
        if int_number_of_bytes == 1:
            return self.dict_map_number_to_ascii[int_value]
        if int_value < 0:
            raise ValueError("Input cannot be negative")
        _int_value = int_value
        _list_output = []
        for int_iteration in range(int_number_of_bytes):
            _int_value_digit = _int_value % CONST_UNIQUE_CHARACTERS_IN_A_BYTE
            _list_output.append(self.dict_map_number_to_ascii[_int_value_digit])
            _int_value = int((_int_value - _int_value_digit) / CONST_UNIQUE_CHARACTERS_IN_A_BYTE)
        _list_output.reverse()
        _str_output = "".join(_list_output)
        return _str_output

    def populate_list_byte_encoded(self, object_target: EncodeObject):
        _int_byte_to_member = object_target.int_byte_to_member
        if _int_byte_to_member >= 1:
            for _int_index in object_target.list_mappings:
                _str_encoded = self.convert_to_byte_representation(_int_index, _int_byte_to_member)
                object_target.list_byte_encoded.append(_str_encoded)


    def populate_list_time_byte_encoded(self, object_time_target: EncodeTimeObject):
        _int_list_iter = 0
        _int_byte_to_member = object_time_target.int_byte_to_member
        if _int_byte_to_member >= 1:
            pass
        # TODO change this
        _int_byte_to_member = max(_int_byte_to_member,1)
        for _list_relative_time in object_time_target.list_mappings:
            object_time_target.list_byte_encoded.append([])
            for _int_relative_time in _list_relative_time:
                _str_encoded = self.convert_to_byte_representation(_int_relative_time, _int_byte_to_member)
                object_time_target.list_byte_encoded[_int_list_iter].append(_str_encoded)
            _int_list_iter += 1

    def put_obj_list_in_output_list(self, object_in : EncodeObject):
        for _item in object_in.list_unique_members:
            self.list_outgoing.append(_item)
            self.list_outgoing.append('\t')
        self.list_outgoing.append('\n')
        self.list_outgoing.append(object_in.int_byte_to_member)
        self.list_outgoing.append('\n')
        self.list_outgoing += object_in.list_byte_encoded
        self.list_outgoing.append('\n')

    def put_time_obj_list_in_output_list(self, obj_time):
        for _int_rep_item in obj_time.list_unique_members:
            self.list_outgoing.append(_int_rep_item)
            self.list_outgoing.append('\t')
        self.list_outgoing.append('\n')
        self.list_outgoing.append(obj_time.int_byte_to_member)
        self.list_outgoing.append('\n')
        for _list in obj_time.list_byte_encoded:
            for _str_item in _list:
                self.list_outgoing.append(_str_item)
            self.list_outgoing.append('\t')
        self.list_outgoing.append('\n')


class SaveToFile:
    def __init__(self, str_path_of_file):
        self.str_name_of_file = "{0}.vzip".format(os.path.splitext(str_path_of_file)[0])

    def write_to_file(self, list_content):
        file = open(self.str_name_of_file, 'w')
        str_content = ''
        for element in list_content:
            str_content += str(element)
        file.write(str_content)
        file.close()


def main():
    file_path = input("Enter the complete path to the input ebat file, including the file name and its extension:\n")    

    reader = CSVReader(file_path)

    obj_get_first_set_of_stats = GetFirstSetOfStats()

    for row in reader:
        obj_get_first_set_of_stats.update_dict_unique_tickers_to_minimum_price(row)

        obj_get_first_set_of_stats.get_time_gaps(obj_get_first_set_of_stats.ObjTimeFIRSTEncode, row)
        obj_get_first_set_of_stats.get_time_gaps(obj_get_first_set_of_stats.ObjRepTimeFIRSTEncode, row)

        obj_get_first_set_of_stats.add_to_list(obj_get_first_set_of_stats.ObjExchangeFIRSTEncode, row)
        obj_get_first_set_of_stats.add_to_list(obj_get_first_set_of_stats.ObjSideFIRSTEncode, row)
        obj_get_first_set_of_stats.add_to_list(obj_get_first_set_of_stats.ObjConditionFIRSTEncode, row)
        obj_get_first_set_of_stats.add_to_list(obj_get_first_set_of_stats.ObjSizeFIRSTEncode, row)

    set_secondary_stats = GetSecondSetOfStats(obj_get_first_set_of_stats)

    set_secondary_stats.populate_lists_of_unique_tickers_and_minimum_price()

    reader = CSVReader(file_path)
    set_secondary_stats.perform_first_encoding_of_object(set_secondary_stats.ObjTickerFIRSTEncode)
    set_secondary_stats.perform_first_encoding_of_object(set_secondary_stats.ObjExchangeFIRSTEncode)
    set_secondary_stats.perform_first_encoding_of_object(set_secondary_stats.ObjSideFIRSTEncode)
    set_secondary_stats.perform_first_encoding_of_object(set_secondary_stats.ObjConditionFIRSTEncode)
    set_secondary_stats.perform_first_encoding_of_object(set_secondary_stats.ObjSizeFIRSTEncode)

    set_secondary_stats.perform_first_encoding_of_time_object(set_secondary_stats.ObjTimeFIRSTEncode)
    set_secondary_stats.perform_first_encoding_of_time_object(set_secondary_stats.ObjRepTimeFIRSTEncode)
    set_secondary_stats.perform_first_encoding_of_price_object()

    set_secondary_stats.populate_dict_item_to_index(set_secondary_stats.ObjExchangeFIRSTEncode)
    set_secondary_stats.populate_dict_item_to_index(set_secondary_stats.ObjSideFIRSTEncode)
    set_secondary_stats.populate_dict_item_to_index(set_secondary_stats.ObjConditionFIRSTEncode)
    set_secondary_stats.populate_dict_item_to_index(set_secondary_stats.ObjSizeFIRSTEncode)
    set_secondary_stats.populate_dict_item_to_index(set_secondary_stats.ObjTickerFIRSTEncode)

    for row in reader:
        set_secondary_stats.perform_second_encoding_of_object(set_secondary_stats.ObjTickerFIRSTEncode, row)
        set_secondary_stats.perform_second_encoding_of_object(set_secondary_stats.ObjExchangeFIRSTEncode, row)
        set_secondary_stats.perform_second_encoding_of_object(set_secondary_stats.ObjSideFIRSTEncode, row)
        set_secondary_stats.perform_second_encoding_of_object(set_secondary_stats.ObjConditionFIRSTEncode, row)
        set_secondary_stats.perform_second_encoding_of_object(set_secondary_stats.ObjSizeFIRSTEncode, row)

        set_secondary_stats.perform_second_encoding_of_price_object(row)

    set_third_set_of_stats = GetThirdSetOfStats(set_secondary_stats,obj_get_first_set_of_stats)
    set_third_set_of_stats.populate_dict_map_number_to_ascii()

    set_third_set_of_stats.populate_list_byte_encoded(set_third_set_of_stats.ObjTickerFIRSTEncode)
    set_third_set_of_stats.populate_list_byte_encoded(set_third_set_of_stats.ObjExchangeFIRSTEncode)
    set_third_set_of_stats.populate_list_byte_encoded(set_third_set_of_stats.ObjSideFIRSTEncode)
    set_third_set_of_stats.populate_list_byte_encoded(set_third_set_of_stats.ObjConditionFIRSTEncode)
    set_third_set_of_stats.populate_list_byte_encoded(set_third_set_of_stats.ObjRelativePriceFIRSTEncode)
    set_third_set_of_stats.populate_list_byte_encoded(set_third_set_of_stats.ObjSizeFIRSTEncode)

    set_third_set_of_stats.populate_list_time_byte_encoded(set_third_set_of_stats.ObjTimeFIRSTEncode)
    set_third_set_of_stats.populate_list_time_byte_encoded(set_third_set_of_stats.ObjRepTimeFIRSTEncode)

    set_third_set_of_stats.put_obj_list_in_output_list(set_third_set_of_stats.ObjTickerFIRSTEncode)
    set_third_set_of_stats.put_obj_list_in_output_list(set_third_set_of_stats.ObjExchangeFIRSTEncode)
    set_third_set_of_stats.put_obj_list_in_output_list(set_third_set_of_stats.ObjSideFIRSTEncode)
    set_third_set_of_stats.put_obj_list_in_output_list(set_third_set_of_stats.ObjConditionFIRSTEncode)
    set_third_set_of_stats.put_time_obj_list_in_output_list(set_third_set_of_stats.ObjTimeFIRSTEncode)
    set_third_set_of_stats.put_time_obj_list_in_output_list(set_third_set_of_stats.ObjRepTimeFIRSTEncode)
    set_third_set_of_stats.put_obj_list_in_output_list(set_third_set_of_stats.ObjRelativePriceFIRSTEncode)
    set_third_set_of_stats.put_obj_list_in_output_list(set_third_set_of_stats.ObjSizeFIRSTEncode)

    obj_save_ebat = SaveToFile(file_path)
    obj_save_ebat.write_to_file(set_third_set_of_stats.list_outgoing)

if __name__ == '__main__':
    main()
