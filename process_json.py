import json
import sys
from datetime import datetime


class DataType():

    def __init__(self, field_key: str = 0, field_value: str = "", data_type: str = "") -> None:
        self.field_key = field_key
        self.field_value = field_value
        self.data_type = data_type
        self.invalid = True

    def strip_trailing_leading_space(self) -> tuple:
        return (self.field_key.strip(), self.field_value.strip(), self.data_type.strip())

    def omit_fields_with_empty_key_value(self) -> bool:
        if not self.field_key:
            return False # skip the field
        return True

    def omit_all_invalid_fields(self) -> bool:
        if self.invalid:
            return False # skip the field
        return True


class NumericDataType(DataType):

    def __init__(self, field_key: str = 0, field_value: str = "", data_type: str = "") -> None:
        super.__init__(field_key, field_value, data_type)

    def omit_all_invalid_fields(self) -> bool:
        if self.field_value.isnumeric():
            self.invalid = True  # skip the field
            return True
        return False
    
    def must_strip_leading_zero(self) -> str:
        if self.field_value.startswith("0"):
            new_string = self.field_value.lstrip("0")
            return new_string
        return self.field_value



class StringDataType(DataType):
    def __init__(self, field_key: str = 0, field_value: str = "", data_type: str = "") -> None:
        super.__init__(field_key, field_value, data_type)

    def omit_fields_with_empty_key_value(self) -> bool:
        if not self.field_value:
            self.invalid = True  # skip the field
            return False  
        return True
    
    def valid_rfc3339_to_epoch(self):
        timestamp_str = self.field_value
        try:
            timestamp = datetime.fromisoformat(timestamp_str)
            print(f"Valid RFC timestamp: {timestamp}")
            epoch_time = int(timestamp_str.timestamp())
            print(f"Unix epoch time: {epoch_time}")
            self.field_value = epoch_time

        except ValueError:
            print(f"{self.field_value} is not RFC3339 timestamp")


class BoolDataType(DataType):
    def __init__(self, field_key: str = 0, field_value: str = "", data_type: str = "") -> None:
        super.__init__(field_key, field_value, data_type)

    def transform_true_false(self) -> bool:
        if self.field_value in ["1", "t", "T", "TRUE", "true", "True"]:
            self.field_value = True
            self.invalid = False
            return True
        elif self.field_value in ["0", "f", "F", "FALSE", "false", "False"]:
            self.field_value = False
            self.invalid = False
        else:
            self.invalid = True


class NullDataType(DataType):
    def __init__(self, field_key: str = 0, field_value: str = "", data_type: str = "") -> None:
        super.__init__(field_key, field_value, data_type)

    def transform_true_false(self) -> bool:
        if self.field_value in ["1", "t", "T", "TRUE", "true", "True"]:
            self.field_value = None
            self.invalid = False
        elif self.field_value in ["0", "f", "F", "FALSE", "false", "False"]:
            self.invalid = True


# class ListDataType():
#     ListField = namedtuple(
#         'ListField', ['json_key', 'data_type', 'data_value'])


# class MapDataType():
#     MapField = namedtuple('MapField', ['json_key', 'data_type', 'data_value'])


class Transformation():

    def __init__(self, json_file: str) -> None:

        self.json_file = json_file
        self.datatype_dict = {}

    def read_file(self):

        f = None
        try:
            f = open(self.json_file)

            json_data = f.read()
            self.datatype_dict = json.loads(json_data)

        except IOError as ioe:
            print(f"IOError: {ioe}")

        finally:
            f.close()

    def transform_data(self):
        print(self.datatype_dict)

    def write_file(self):

        json_data = json.dumps(self.datatype_dict)

        f = None
        try:
            f = open('output_file.json', 'w')

            f.write(json_data)

        except IOError as ioe:
            print(f"IOError: {ioe}")

        finally:
            f.close()


def main() -> None:

    if len(sys.argv) != 2:
        print(f"{sys.argv[0]} <input json file>")
        sys.exit(1)

    json_file = sys.argv[1].strip()
    d = DataType(json_file)
    d.read_file()
    d.transform_data()
    d.write_file()


if __name__ == '__main__':
    main()
