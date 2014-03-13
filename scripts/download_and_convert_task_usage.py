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
import re
import fileinput
import subprocess
import logging
import sys

logging.basicConfig(level=logging.DEBUG)


class Downloader:
    def __init__(self, resource_name, gsutil_path, first_file=0, last_file=499):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._remote_pattern = 'gs://clusterdata-2011-1/{}/part-{}-of-00500.csv.gz'.format(resource_name, '{}')
        self._local_pattern = 'download/{}/part-{}-of-00500.csv.gz'.format(resource_name, '{}')
        self._gsutil_path = gsutil_path
        self._opened_file = None
        self._opened_input = None
        self._file_index = first_file - 1
        self._last_file = last_file

    def download_next(self):
        if self._opened_file != None:
            self._close()

        self._file_index += 1

        if self._file_index <= self._last_file:
            self._opened_file = self._download_and_extract(self._file_index)
            self._logger.info('Opening file: %s', self._opened_file)
            self._opened_input = fileinput.input(self._opened_file)
            return self._opened_input

        self._logger.info('No more files to open.')
        self._opened_file = None
        self._opened_input = None
        return None

    def _close(self):
        self._logger.info('Closing file: %s', self._opened_file)
        self._opened_input.close()
        os.remove(self._opened_file)
        self._opened_file = None
        self._opened_input = None

    def _download_and_extract(self, file_index):
        self._logger.info('Downloading file: %s', self._remote_pattern.format(str(file_index).zfill(5)))
        down_comm = './{}/gsutil cp {} {}'.format(self._gsutil_path,
                                                  self._remote_pattern.format(str(file_index).zfill(5)),
                                                  self._local_pattern.format(str(file_index).zfill(5)))
        subprocess.call(down_comm, shell=True)

        extr_comm = 'gzip -d {}'.format(self._local_pattern.format(str(file_index).zfill(5)))
        subprocess.call(extr_comm, shell=True)

        return re.match('^(.*)\.gz$', self._local_pattern.format(str(file_index).zfill(5))).group(1)


class FileSetReader:
    def __init__(self, downloader):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._downloader = downloader
        self._line = None
        self._input = None

    def initialize(self):
        self._input = self._downloader.download_next()

    def next_line(self):
        self._line = self._input.readline()
        if len(self._line) == 0:
            self._input = self._downloader.download_next()
            if self._input == None:
                return None
            self._line = self._input.readline()
        return self._line

    def current_line(self):
        return self._line


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

    def print_line_to_vm(self, line, job_id, vm_name):
        os.makedirs('{}/{}/'.format(self._output_dir, job_id), exist_ok=True)
        output = open('{}/{}/{}.csv'.format(self._output_dir, job_id, vm_name), "a")
        output.write(line + '\n')
        output.close()


class TaskUsageFilterer:
    def __init__(self, downloader, output_directory):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._output_dir = output_directory
        self._fileset = FileSetReader(downloader)
        self._output = UsageOutput(output_directory)

    def filter(self):
        self._fileset.initialize()

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
            self._output.print_line_to_vm(new_usage, job_id, vm_name)
            line = self._fileset.next_line()


# main:
output_dir = sys.argv[1]
gsutil_path = sys.argv[2]

downloader = Downloader('task_usage', gsutil_path)

filterer = TaskUsageFilterer(downloader, output_dir)
filterer.filter()
