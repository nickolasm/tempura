import csv
import string
import cx_Oracle
import datetime as dt
import os
import math


class SimpleAnalyser(object):
    def __init__(self, filename, config, logger, debug_flag):
        self.filename = filename
        self.config = config
        self.logger = logger
        self.debug = debug_flag
        self.date_formats = (
            '%Y',
            '%b %d, %Y',
            '%b %d, %Y',
            '%B %d, %Y',
            '%B %d %Y',
            '%m/%d/%Y',
            '%m/%d/%y',
            '%b %Y',
            '%B%Y',
            '%b %d,%Y'
        )

    def __enter__(self):
        self.data_header = dict()
        self.data_rows = dict()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.data_header = None
        self.data_rows = None

    def roundup(self, x):
        return int(math.ceil(x / 10.0)) * 10

    def date_oracle(self, string_):
        return string_.replace('%Y', 'YYYY').replace('%y','YY').\
            replace('%m', 'MM').replace('%b', 'MON').replace('%B','MONTH').\
            replace('%d', 'DD')

    def is_number(self, s):
        if ',' in s:
            commas_included = True
        else:
            commas_included = False
        try:
            float(s.replace(',',''))
            return True, commas_included
        except ValueError:
            pass

        try:
            import unicodedata
            unicodedata.numeric(s.replace(',',''))
            return True, commas_included
        except (TypeError, ValueError):
            pass

        return False, commas_included

    def is_date(self, string_to_parse):
        parsed_date = []
        for date_format in self.date_formats:
            try:
                t = dt.datetime.strptime(string_to_parse, date_format)
                parsed_date.append(date_format)
                break
            except ValueError as err:
                pass

        return len(parsed_date) > 0, parsed_date

    def perform_file_analysis(self):

        header_parsed = False

        with (open(self.filename, 'r')) as sf:
            reader = csv.reader(sf, delimiter=self.config['delimiter'], quotechar=self.config['quote_char'])

            for line_count, line_data in enumerate(reader):
                if self.debug:
                    msg = 'Line[{}] = {}'.format(line_count, line_data)
                    self.logger.debug(msg)
                    print(msg)

                if not header_parsed and self.config['has_header']:
                    temp_data_header = dict()
                    for element_count, element_data in enumerate(line_data):

                        element = dict()
                        element['original'] = element_data
                        element['name'] = str(element_data).strip().upper().replace(' ', '_')
                        element['length'] = len(element['name'])

                        invalidChars = set(string.punctuation.replace("_", ""))
                        if any(char in invalidChars for char in element['name']):
                            element['Status'] = 'Invalid'
                        else:
                            element['Status'] = 'Valid'

                        temp_data_header[element_count] = element

                    self.data_header = temp_data_header
                    header_parsed = True
                    if self.debug:
                        msg = 'HEADER: {}'.format(self.data_header)
                        self.logger.info(msg)
                        print(msg)

                else:

                    temp_data_row = dict()
                    for element_count, element_data in enumerate(line_data):

                        element = dict()
                        element['data'] = element_data
                        element['size'] = len(element_data)

                        test_string = str(element_data)
                        IS_NUMBER, WITH_COMMAS = self.is_number(test_string)

                        if IS_NUMBER:
                            element['type'] = cx_Oracle.NUMBER
                            element['with_commas'] = WITH_COMMAS

                        elif test_string.isalnum() or test_string.isalpha() or test_string.isprintable():

                            IS_DATE, Formats = self.is_date(test_string)
                            if not IS_DATE:
                                element['type'] = cx_Oracle.STRING
                            else:
                                element['type'] = cx_Oracle.DATETIME
                                element['datetime_format'] = Formats

                        temp_data_row[element_count] = element

                    self.data_rows[line_count] = temp_data_row

        self.column_sizes, self.column_types, self.rows, self.columns, self.number_with_comma, self.date_formats = \
            self.consintency_check_and_determine_columns()

        self.logger.info('Data rows: {}, Data Columns: {}'.format(self.rows, self.columns))

    def consintency_check_and_determine_columns(self):

        columns = len(self.data_header)
        rows = len(self.data_rows)

        array_size = [[0] * columns for i in range(rows)]
        array_type = [[0] * columns for i in range(rows)]
        max_size = [0] * columns
        eval_type = [True] * columns
        number_with_comma = [False] * columns
        date_formats = [''] * columns

        for row_counter, row in self.data_rows.items():
            if len(row) != len(self.data_header):
                print('Row {} has {} number of elements whereas header has {}'.format(
                    row_counter, len(row), len(self.data_header)))

            for element_counter, element in row.items():
                array_size[row_counter - 1][element_counter] = element['size']
                array_type[row_counter - 1][element_counter] = element['type']
                if 'with_commas' in element:
                    number_with_comma[element_counter] = element['with_commas']
                if 'datetime_format' in element:
                    date_formats[element_counter] = element['datetime_format']

        row_counter = 0
        while row_counter < rows:
            column_counter = 0
            while column_counter < columns:
                if array_size[row_counter][column_counter] > max_size[column_counter]:
                    max_size[column_counter] = array_size[row_counter][column_counter]
                if row_counter > 0:
                    if array_type[row_counter][column_counter] == array_type[row_counter - 1][column_counter]:
                        eval_type[column_counter] = eval_type[column_counter] & True
                    else:
                        eval_type[column_counter] = eval_type[column_counter] & False
                column_counter += 1
            row_counter += 1

        if all(item for item in eval_type):
            ret = array_type[0]
        else:
            ret =  None

        return max_size, ret, rows, columns, number_with_comma, date_formats

    def generate_template(self):
        filepath, ext = os.path.splitext(self.filename)
        path, name = os.path.split(filepath)

        prolog = """load data
INFILE 'loader2.dat'
INTO TABLE articles_formatted
APPEND
FIELDS TERMINATED BY ','
("""
        epilog = """)"""

        with open('{}_TEMPLATE.txt'.format(filepath), 'w') as out:

            out.write('{}\n'.format(prolog))

            column_counter = 0
            while column_counter < self.columns:

                item_type = self.column_types[column_counter]
                item = self.data_header[column_counter]['name']
                ora_command = ''
                if item_type == cx_Oracle.STRING:
                    ora_command = '"TRIM(:{})"'.format(item)
                elif item_type == cx_Oracle.NUMBER:
                    comma = self.number_with_comma[column_counter]
                    if comma:
                        ora_command = '"TO_NUMBER(TRIM(:{}), \'999,999,999.99\')"'.format(item)
                    else:
                        ora_command = '"TO_NUMBER(TRIM(:{}))"'.format(item)
                elif item_type == cx_Oracle.DATETIME:
                    format = self.date_formats[column_counter]
                    ora_command = '"TO_DATE(TRIM(:{}), \'{}\')"'.format(item, self.date_oracle(format.pop(0)))

                out.write('{}\t\t\t{},\n'.format(item, ora_command))

                column_counter += 1

            out.write('{}\n'.format(epilog))

    def generate_import_table(self):
        filepath, ext = os.path.splitext(self.filename)
        path, name = os.path.split(filepath)

        prolog = 'CREATE TABLE PSN_IMP_{}\n('.format(name.replace(' ', '_').upper())
        epilog = ');'

        with open('{}_DML.sql'.format(filepath), 'w') as out:

            out.write('{}'.format(prolog))
            string_to_write = ''
            column_counter = 0
            while column_counter < self.columns:

                item_type = self.column_types[column_counter]
                item = self.data_header[column_counter]['name']
                item_size = self.roundup(self.column_sizes[column_counter])
                if item_size == 0:
                    ora_def_val = ' DEFAULT NULL'
                else:
                    ora_def_val = ''

                ora_declare = ''
                if item_type == cx_Oracle.STRING:
                    ora_declare = 'VARCHAR2({}){}'.format(item_size, ora_def_val)
                elif item_type == cx_Oracle.NUMBER:
                    ora_declare = 'NUMBER{}'.format(ora_def_val)
                elif item_type == cx_Oracle.DATETIME:
                    ora_declare = 'DATE{}'.format(ora_def_val)

                string_to_write += '\n{}\t\t\t{},'.format(item, ora_declare)

                column_counter += 1

            out.write(string_to_write.rstrip(','))
            out.write('\n{}\n'.format(epilog))
