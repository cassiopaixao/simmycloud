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


class UsageOutput:
    def __init__(self, output_directory):
        self._output_dir = output_directory
        try:
            os.makedirs(self._output_dir)
        except OSError:
            raise Exception("Output directory '%s' already exists. Can't filter due to possible conflicts." % self._output_dir)
        self._clear()

    def _clear(self):
        self._out = None

    def print_line_to_vm(self, line, vm_name):
        output = open('{}/{}.csv'.format(self._output_dir, vm_name), "a")
        output.write(line + '\n')
        output.close()


class TaskUsageFilterer:
    def __init__(self, input_directory, output_directory):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._input_dir = input_directory
        self._output_dir = output_directory
        self._fileset = FileSetReader()
        self._output = UsageOutput(output_directory)

    def filter(self):
        self._fileset.initialize(self._input_dir)

        line = self._fileset.next_line()
        while line is not None:
            # start_time,end_time,job_id,task_index,machine_id,cpu_usage,
            # memory_usage,assigned_memory,unmapped_page_cache_memory_usage,
            # page_cache_memory_usage,maximum_memory_usage,disk_io_time_mean,
            # local_disk_space_used,cpu_rate,disk_io_time_max,cpi,mai,
            # sampling_rate,aggregation_type
            data = line.strip().split(',')
            start_time = data[0]
            end_time = data[1]
            job_id = data[2]
            task_index = data[3]
            cpu_usage = float(data[5])
            mem_usage = float(data[10]) if data[10] != '' else float(data[6])

            vm_name = '{}-{}'.format(job_id, task_index)

            # start_time,end_time,cpu_usage,mem_usage
            new_usage = '{start_time},{end_time},{cpu_usage},{mem_usage}'.format(
                start_time = start_time,
                end_time = end_time,
                cpu_usage = cpu_usage,
                mem_usage = mem_usage
                )
            self._output.print_line_to_vm(new_usage, vm_name)

            line = self._fileset.next_line()


# main:
input_dir = sys.argv[1]
output_dir = sys.argv[2]

filterer = TaskUsageFilterer(input_dir, output_dir)
filterer.filter()
