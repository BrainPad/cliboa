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
import re


class File(object):
    def remove_csv_col(self, input_file, output_file, remains, enc="utf-8"):
        """
        Extract only the necessary columns from the CSV data and output a new CSV

        Args:
            input_file (str): Input csv file name
            output_file (str): Output csv file name
            remains (str[]): Columns which remain for new csv
            enc=utf-8 (str): encording
        """
        with codecs.open(input_file, mode="r", encoding=enc) as in_f, codecs.open(
            output_file, mode="w", encoding="utf-8"
        ) as out_f:
            reader = csv.DictReader(in_f)
            writer = csv.writer(out_f)
            writer.writerow(remains)

            for row in reader:
                contents = []
                for col in remains:
                    contents.append(row[col])
                writer.writerow(contents)
            out_f.flush()

    def get_target_files(self, src_dir, src_pattern, tree=True):
        """
        Get files which matches to the regular expression

        Args:
            src_dir (str): Directory to search
            src_pattern (str): Regular expression
            tree=True (bool): Set True(by default)
                              to search files include sub directories.

        Returns:
            list: Matched file list
        """
        r = re.compile(src_pattern)
        target_files = []
        for dir, dirs, files in os.walk(src_dir):
            if tree is False and dir not in src_dir:
                continue
            for file in files:
                if r.fullmatch(file):
                    target_files.append(os.path.join(dir, file))
        return sorted(target_files)

    def convert_encoding(self, src, dest, encoding_from, encoding_to, errors=None):
        """
        Copy file with specified encoding

        Args:
            src (str): Copy source file name
            dest (str): Copy destination file name
            enc_from (str): Encoding of source file
            enc_to (str): Encoding of destination file
        """
        with open(src, "r", encoding=encoding_from, errors=errors) as input:
            with open(dest, "w", encoding=encoding_to, errors=errors) as output:
                for i in input:
                    output.write(i)
