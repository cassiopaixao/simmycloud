###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2013 Cassio Paixao
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
import math

logging.basicConfig(level=logging.DEBUG)


class FileSetReader:
    def __init__(self, input_directory):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._input_directory = input_directory
        self._clear()

    def _clear(self):
        self._opened_file = None
        self._file_index = -1
        self._line = None

    def initialize(self):
        self._clear()
        self._folders = os.listdir(self._input_directory)
        self._folders[:] = sorted(self._folders)
        self._folders[:] = [self._input_directory + '/' + folder for folder in self._folders]
        self._folder_i = -1

    def next_folder(self):
        self._folder_i = self._folder_i + 1
        if self._folder_i >= len(self._folders): return None

        self._logger.debug('Next folder: %20d %s', self._folder_i, self._folders[self._folder_i])

        return self._folders[self._folder_i]

    def load_folder(self, folder):
        self._files = os.listdir(folder)
        self._files[:] = sorted(self._files)
        self._files[:] = [folder + '/' + filename for filename in self._files]
        self._file_i = -1

    def next_file(self):
        self._file_i = self._file_i + 1
        if self._file_i >= len(self._files): return None

        self._logger.debug('Next file  : %20d %s', self._file_i, self._files[self._file_i])

        return self._files[self._file_i]


class StatisticsOutput:
    def __init__(self, output_file):
        self._output_file = output_file

    def print_line(self, line):
        output = open(self._output_file, "a")
        output.write(line + '\n')
        output.close()


class TaskUsageStatistics:
    def __init__(self, input_directory, output_file):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._fileset = FileSetReader(input_directory)
        self._output = StatisticsOutput(output_file)

    def run(self):
        self._fileset.initialize()

        self._print_header()

        folder = self._fileset.next_folder()

        while folder is not None:
            self._fileset.load_folder(folder)
            filename = self._fileset.next_file()

            while filename is not None:
                opened_file = fileinput.input(filename)
                self._initialize_stats(filename)
                line = opened_file.readline()
                while len(line) > 0:
                    self._process_line(line)
                    line = opened_file.readline()
                self._persist_stats()

                filename = self._fileset.next_file()
            folder = self._fileset.next_folder()

    def _initialize_stats(self, filename):
        self.__first_time = None
        self.__last_time = 0
        # self.__duration = 0
        self.__job_id = re.match('.*/(\d+)-\d+.csv', filename).group(1)
        self.__vm_name = re.match('.*/([\d-]+).csv', filename).group(1)
        self.__min_cpu = 2.0
        self.__min_mem = 2.0
        self.__min_vec = 2.0
        self.__max_cpu = 0.0
        self.__max_mem = 0.0
        self.__max_vec = 0.0
        # self.__range = 0
        # self.__mean = 0
        # self.__std_dev = 0
        self.__measurements = []

    def _process_line(self, line):
        start_time, end_time, cpu, mem = line.split(',')

        cpu, mem = float(cpu), float(mem)
        vec = math.sqrt(math.pow(cpu, 2.0) + math.pow(mem, 2.0))

        if self.__first_time is None: self.__first_time = start_time
        self.__last_time = end_time

        self.__measurements.append((cpu, mem, vec))

        if self.__min_cpu > cpu: self.__min_cpu = cpu
        if self.__max_cpu < cpu: self.__max_cpu = cpu

        if self.__min_mem > mem: self.__min_mem = mem
        if self.__max_mem < mem: self.__max_mem = mem

        if self.__min_vec > vec: self.__min_vec = vec
        if self.__max_vec < vec: self.__max_vec = vec

    def _persist_stats(self):
        sum_cpu, sum_mem, sum_vec =\
            math.fsum([m[0] for m in self.__measurements]),\
            math.fsum([m[1] for m in self.__measurements]),\
            math.fsum([m[2] for m in self.__measurements])

        mean_cpu, mean_mem, mean_vec =\
            sum_cpu / len(self.__measurements),\
            sum_mem / len(self.__measurements),\
            sum_vec / len(self.__measurements),

        std_dev_cpu = self._std_dev('cpu', mean_cpu)
        std_dev_mem = self._std_dev('mem', mean_mem)
        std_dev_vec = self._std_dev('vec', mean_vec)

        vm_stats = ','.join([str(value) for value in [self.__first_time,
                    self.__last_time,
                    int(self.__last_time) - int(self.__first_time), #duration
                    self.__job_id,
                    self.__vm_name,
                    len(self.__measurements),
                    self.__min_cpu,
                    self.__max_cpu,
                    self.__max_cpu - self.__min_cpu, #range
                    mean_cpu,
                    std_dev_cpu,
                    self.__min_mem,
                    self.__max_mem,
                    self.__max_mem - self.__min_mem, #range
                    mean_mem,
                    std_dev_mem,
                    self.__min_vec,
                    self.__max_vec,
                    self.__max_vec - self.__min_vec, #range
                    mean_vec,
                    std_dev_vec]])

        self._output.print_line(vm_stats)

    def _print_header(self):
        header = ','.join(['first_time',
            'last_time',
            'duration',
            'job_id',
            'vm_name',
            'measurements',
            'min_cpu',
            'max_cpu',
            'range_cpu',
            'mean_cpu',
            'std_dev_cpu',
            'min_mem',
            'max_mem',
            'range_mem',
            'mean_mem',
            'std_dev_mem',
            'min_vec',
            'max_vec',
            'range_vec',
            'mean_vec',
            'std_dev_vec'])
        self._output.print_line(header)

    def _std_dev(self, resource, mean):
        res_index = {'cpu':0, 'mem':1, 'vec':2}[resource]
        difference_to_mean_sq = [math.pow(v[res_index]-mean, 2.0) for v in self.__measurements]
        std_dev = math.sqrt(math.fsum(difference_to_mean_sq)/(len(self.__measurements) - 1))
        return std_dev


# main:
logger = logging.getLogger(__name__)
input_dir = sys.argv[1]
output_file = sys.argv[2]

logger.debug('Input directory: {}'.format(input_dir))
logger.debug('Output file: {}'.format(output_file))

statistics = TaskUsageStatistics(input_dir, output_file)
logger.debug('Running script')
statistics.run()
logger.debug('Ended script')
