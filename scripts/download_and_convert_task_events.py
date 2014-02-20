import os
import fileinput
import re
import subprocess
import logging
import sys

logging.basicConfig(level=logging.INFO)

SCHEDULE_EVENT = '0'
FINISH_EVENT = '1'

LAST_TIMESTAMP = None

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


class DownloaderFileReader:
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


class TaskEventsPreFilterer:
    def __init__(self, downloader, output_directory):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._output_dir = output_directory
        self._fileset = DownloaderFileReader(downloader)
        self._output = FiltererOutput(output_directory)

    def filter(self):
        global LAST_TIMESTAMP

        self._fileset.initialize()

        line = self._fileset.next_line()
        while line is not None:
            # timestamp,missing_info,job_id,task_index,machine_index,
            # event_type,username,scheduling_class,priority,
            # resource_request_cpu,resource_request_ram,
            # resource_request_disk_space,different-machine_constraint
            data = line.strip().split(',')
            timestamp = data[0]
            job_id = data[2]
            task_index = data[3]
            event_type = data[5]
            resource_request_cpu = data[9]
            resource_request_mem = data[10]

            LAST_TIMESTAMP = timestamp

            if event_type in ['1','2','3','4','5','6']:
                #timestamp,event_type,vm_name,cpu,mem
                new_event = '{timestamp},{event_type},{vm_name},{cpu},{mem}'.strip().format(
                        timestamp = timestamp,
                        event_type = SCHEDULE_EVENT if event_type == '1' else FINISH_EVENT,
                        vm_name = '{}-{}'.format(job_id, task_index),
                        cpu = resource_request_cpu,
                        mem = resource_request_mem
                    )
                self._output.print_line(new_event)

            line = self._fileset.next_line()
        self._output.close()


class TaskEventsFilterer:
    def __init__(self, input_directory, output_directory):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._input_dir = input_directory
        self._output_dir = output_directory
        self._fileset = FileSetReader()
        self._output = FiltererOutput(output_directory, 1000000)
        self._vm_events = dict()

    def _process_events(self):
        self._fileset.initialize(self._input_dir)

        line = self._fileset.next_line()
        while line is not None:
            # timestamp,event_type,vm_name,cpu,mem
            data = line.split(',')
            e = Event()
            vm_name = data[2]
            e.timestamp = data[0]
            e.event_type = data[1]
            e.cpu = data[3]
            e.mem = data[4]

            if vm_name not in self._vm_events:
                self._vm_events[vm_name] = list()

            self._vm_events[vm_name].append(e)

            line = self._fileset.next_line()

    def filter(self):
        self._process_events()

        self._logger.info('last_timestamp: %d', int(LAST_TIMESTAMP))

        vms_tup = list()
        for vm_name in self._vm_events.keys():
            schedule_timestamp = None
            for event in self._vm_events[vm_name]:
                if event.event_type == SCHEDULE_EVENT and \
                   (schedule_timestamp is None or event.timestamp < schedule_timestamp):
                   schedule_timestamp = event.timestamp
            if schedule_timestamp is not None:
                vms_tup.append((vm_name, schedule_timestamp))

        vms_tup[:] = sorted(vms_tup, key=lambda tup: int(tup[1]))

        self._logger.info('# virtual machines: %d', len(vms_tup))

        for vm_tup in vms_tup:
            vm_name = vm_tup[0]
            events = sorted(self._vm_events[vm_name], key=lambda e: int(e.timestamp))

            self._logger.debug('VM %s has %d events.', vm_name, len(events))

            schedule_event = None
            finish_event = None
            for e in events:
                if e.event_type == SCHEDULE_EVENT and schedule_event is None:
                    schedule_event = e
                elif e.event_type == FINISH_EVENT and finish_event is None:
                    finish_event = e

            if schedule_event is None:
                self._logger.info('VM %s doesn\'t have a schedule event.', vm_name)
                for e in events:
                    self._logger.debug('%s\'s event: ts: %s, ev_ty: %s', vm_name, e.timestamp, e.event_type)
                continue

            if finish_event is not None:
                finish_timestamp = int(finish_event.timestamp)
            else:
                finish_timestamp = int(LAST_TIMESTAMP)

            new_event = '{timestamp},{vm_name},{cpu},{mem},{process_time}'.strip().format(
                        timestamp= schedule_event.timestamp,
                        vm_name = vm_name,
                        cpu = float(schedule_event.cpu),
                        mem = float(schedule_event.mem),
                        process_time = (finish_timestamp - int(schedule_event.timestamp))
                    )
            self._output.print_line(new_event)
        self._output.close()


class Event:
    def __init__(self):
        self.timestamp = None
        self.event_type = None
        self.cpu = None
        self.mem = None


# main:
output_dir = sys.argv[1]
gsutil_path = sys.argv[2]

downloader = Downloader('task_events', gsutil_path)

prefilterer = TaskEventsPreFilterer(downloader, 'tmp')
prefilterer.filter()

filterer = TaskEventsFilterer('tmp', output_dir)
filterer.filter()
