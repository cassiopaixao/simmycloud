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
import subprocess
import sys

# resource_to_download = 'task_usage'
# resource_to_download = 'task_events'
# last_part = 499
# start_from = 0

if len(sys.argv) < 4:
	print('Usage: python3 {} RESOURCE_TO_DOWNLOAD START_FROM LAST_PART\n'.format(sys.argv[0]))
	exit()


resource_to_download = sys.argv[1]
start_from = int(sys.argv[2])
last_part = int(sys.argv[3])

remote_path = 'gs://clusterdata-2011-1/' + resource_to_download + '/'
local_path = 'download/clusterdata-2011-1/' + resource_to_download + '/'

arquivos = os.listdir(os.path.expanduser(local_path))

# verifica o ultimo arquivo baixado
for arq in arquivos:
	try:
		int(arq.split('-')[1])
	except:
		continue
	if (int(arq.split('-')[1]) > start_from and int(arq.split('-')[1]) < last_part):
		start_from = int(arq.split('-')[1])

# comeca a baixar pelo ultimo arquivo (nao verifica se esta completo ou incompleto.. so baixa de novo..)
for i in range(start_from, last_part+1):
	filename = 'part-{}-of-00500.csv.gz'.format(str(i).zfill(5))
	remote_file = remote_path + filename
	local_file = local_path + filename
	print('./gsutil cp {} {}'.format(remote_file, local_path))
	subprocess.call('./gsutil cp {} {}'.format(remote_file, local_path), shell=True)
