import os
import fileinput
import re
import logging
import sys

logging.basicConfig(level=logging.DEBUG)

class FileSetReader:
    def __init__(self):
        self._clear()
        self._logger = logging.getLogger(self.__class__.__name__)

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
            self._logger.debug('Closing file: %s', self._opened_file._filename)
            self._opened_file.close()
        self._file_index += 1
        if self._file_index < len(self._files):
            self._logger.debug('Opening file: %s', self._files[self._file_index])
            self._opened_file = fileinput.input(self._files[self._file_index])
        else:
            self._logger.debug('No more files to open.')
            self._opened_file = None


class FiltererOutput:
    def __init__(self, output_directory, lines_per_file=1000000):
        self.lines_per_file = lines_per_file
        self.output_dir = output_directory
        try:
            os.makedirs(self.output_dir)
        except OSError:
            raise Exception("Output directory '%s' already exists. Can't filter due to possible conflicts." % self.output_dir)
        self.clear()

    def clear(self):
        self.output_file_number = 0
        self.line_counter = 0
        self.out = None
        self.open_next_file()

    def open_next_file(self):
        if self.out is not None:
            self.out.close()
        self.output_file_number += 1
        self.out = open(("%s/%05d.csv" % (self.output_dir, self.output_file_number)), "w")
        self.line_counter = 0

    def print_line(self, line):
        if self.line_counter == self.lines_per_file:
            self.open_next_file()
        self.out.write(line + '\n')
        self.line_counter += 1

    def close(self):
        self.out.close()


class PreFilterTaskEvents:
    def __init__(self, input_directory, output_directory):
        self._fileset = FileSetReader()
        self.input_dir = input_directory
        self.output_dir = output_directory
        self._logger = logging.getLogger(self.__class__.__name__)
        self._output = FiltererOutput(output_directory, 10000)

    def filter(self):
        self._fileset.initialize(self.input_dir)

        line = self._fileset.next_line()
        while line is not None:
            # timestamp,missing_info,job_id,task_index,machine_index,
            # event_type,username,scheduling_class,priority,
            # resource_request_cpu,resource_request_ram,
            # resource_request_disk_space,different-machine_constraint
            data = line.split(',')
            timestamp = data[0]
            missing_info = data[1]
            job_id = data[2]
            task_index = data[3]
            event_type = data[5]
            resource_request_cpu = data[9]
            resource_request_mem = data[10]

            if event_type in ['1','2','3','4','5','6'] and missing_info == '':
                #timestamp,event_type,vm_name,cpu,mem
                try:
                    new_event = '{timestamp},{event_type},{vm_name},{cpu},{mem}'.strip().format(

                            timestamp= timestamp,
                            event_type= '0' if event_type == '1' else '1',
                            vm_name= '{}-{}'.format(job_id, task_index),
                            cpu= resource_request_cpu,
                            mem= resource_request_mem

                        )
                    self._output.print_line(new_event)
                except Exception as e:
                    self._logger.error('Couldnot output line: {}'.format(line))
                    self._logger.exception(e)
                    raise e

            line = self._fileset.next_line()


input_dir = sys.argv[1]
output_dir = sys.argv[2]

prefilter = PreFilterTaskEvents(input_dir, output_dir)

prefilter.filter()
