
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
