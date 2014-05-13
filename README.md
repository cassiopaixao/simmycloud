SimMyCloud
==========

SimMyCloud is an IaaS infraestructure simulator.

## How to execute

On the simmycloud path, execute

```
$ python3 main.py CONFIG_FILE CONFIG_SECTION
```

where

* CONFIG_FILE is the configuration file to use in execution (there is a config.ini example file)

* CONFIG_SECTION is the configuration section to use in simulation (i.e. first_simulation in config.ini is a configuration section)

## How to create new strategies

For a while, take a look at the strategies folders, all types of strategies has at least one heuristic example.
Remember to use the same decorators, responsible to update the statistics.

## How to change simulation settings

For a while, get a look at the config.ini file.


More information and a user guide
---------------------------------

http://www.ime.usp.br/~cassiop/simmycloud
