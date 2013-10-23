simmycloud
==========

SimMyCloud is an IaaS infraestructure simulator.

## How to execute

On the simmycloud path, execute

[code]
$ python3 main.py CONFIG_FILE [--verify]
[/code]

where

* CONFIG_FILE configuration file to use in execution (more information
will be providade latter. there is a config.ini example file)

* --verify if set, the simulator will only verify if the input data can be
used (e.g. if a FINISH event came before a SUBMIT event to one virtual machine,
so this input data will be considered invalid)

## How to create new strategies

For a while, get a look at the Fake strategies. Remember to use the same
decorators, responsible to update the statistics.

## How to change simulation settings

For a while, get a look at the config.ini file.