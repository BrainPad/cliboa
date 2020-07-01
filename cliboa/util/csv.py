#
# Copyright BrainPad Inc. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
import codecs
import csv
import os


class Csv(object):
    @staticmethod
    def extract_columns_with_names(
        input_file, output_file, remain_column_names, enc="utf-8",
    ):
        """
        Extract only the necessary columns from a CSV file and output a new CSV

        Args:
            input_file: Input csv file name
            output_file: Output csv file name
            remain_column_names: Columns which remain
            enc: Encording
        """
        with codecs.open(input_file, mode="r", encoding=enc) as in_f, codecs.open(
            output_file + ".tmp", mode="w", encoding=enc
        ) as out_f:
            reader = csv.DictReader(in_f)
            writer = csv.writer(out_f)
            writer.writerow(remain_column_names)
            for row in reader:
                contents = []
                for c in remain_column_names:
                    contents.append(row[c])
                writer.writerow(contents)
            out_f.flush()
        os.remove(input_file)
        os.rename(output_file + ".tmp", input_file)

    @staticmethod
    def extract_columns_with_numbers(
        input_file, output_file, remain_column_numbers, enc="utf-8"
    ):
        """
        Extract only the necessary columns from a CSV file and output a new CSV

        Args:
            input_file: Input csv file name
            output_file: Output csv file name
            remain_column_numbers: Column numbers which remain
            enc: Encording
        """
        with codecs.open(input_file, mode="r", encoding=enc) as in_f, codecs.open(
            output_file + ".tmp", mode="w", encoding=enc
        ) as out_f:
            reader = csv.reader(in_f)
            writer = csv.writer(out_f)
            for i, row in enumerate(reader):
                contents = []
                for j, r in enumerate(row):
                    if (j + 1) not in remain_column_numbers:
                        continue
                    contents.append(r)
                writer.writerow(contents)
            out_f.flush()
        os.remove(input_file)
        os.rename(output_file + ".tmp", input_file)
