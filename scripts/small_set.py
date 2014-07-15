###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2013 Cássio Paixão
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
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
###############################################################################

import os
import fileinput
import re
import logging
import sys
import random

logging.basicConfig(level=logging.DEBUG)


class FileSetReader:
    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._clear()

    def _clear(self):
        self._opened_file = None
        self._file_index = -1
        self._line = None

    def initialize(self, input_directory):
        self._clear()
        self._files = os.listdir(input_directory)
        self._files[:] = [f for f in self._files if re.match('.*\.csv$', f) != None]
        self._files[:] = sorted(self._files)
        self._files[:] = [input_directory + '/' + f for f in self._files]

        self._load_next_file()

    def next_line(self):
        self._line = self._opened_file.readline()
        if len(self._line) == 0:
            self._load_next_file()
            if self._opened_file == None:
                return None
            self._line = self._opened_file.readline()
        # self._logger.debug('Line read: %s', self._line.strip())
        return self._line

    def current_line(self):
        return self._line

    def _load_next_file(self):
        if self._opened_file != None:
            self._logger.info('Closing file: %s', self._opened_file._filename)
            self._opened_file.close()
        self._file_index += 1
        if self._file_index < len(self._files):
            self._logger.info('Opening file: %s', self._files[self._file_index])
            self._opened_file = fileinput.input(self._files[self._file_index])
        else:
            self._logger.info('No more files to open.')
            self._opened_file = None


class FiltererOutput:
    def __init__(self, output_directory, lines_per_file=1000000):
        self._lines_per_file = lines_per_file
        self._output_dir = output_directory
        try:
            os.makedirs(self._output_dir)
        except OSError:
            raise Exception("Output directory '%s' already exists. Can't filter due to possible conflicts." % self._output_dir)
        self._clear()

    def _clear(self):
        self._output_file_number = 0
        self._line_counter = 0
        self._out = None
        self._open_next_file()

    def _open_next_file(self):
        if self._out is not None:
            self._out.close()
        self._output_file_number += 1
        self._out = open(("%s/%05d.csv" % (self._output_dir, self._output_file_number)), "w")
        self._line_counter = 0

    def print_line(self, line):
        if self._line_counter == self._lines_per_file:
            self._open_next_file()
        self._out.write(line + '\n')
        self._line_counter += 1

    def close(self):
        self._out.close()


class SmallSetFilterer:
    def __init__(self, input_directory, output_directory, percentage):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._input_dir = input_directory
        self._output_dir = output_directory
        self._percentage = percentage
        self._fileset = FileSetReader()
        self._output = FiltererOutput(output_directory)

    def filter(self):
        self._fileset.initialize(self._input_dir)

        line = self._fileset.next_line()
        while line is not None:

            if random.random() < percentage:
                self._output.print_line(line.strip())

            line = self._fileset.next_line()


# main:
input_dir = sys.argv[1]
output_dir = sys.argv[2]
percentage = float(sys.argv[3])

filterer = SmallSetFilterer(input_dir, output_dir, percentage)
filterer.filter()
