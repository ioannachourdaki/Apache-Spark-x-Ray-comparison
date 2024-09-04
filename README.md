# Apache-Spark-x-Ray-comparison

## Authors
----------------------------------------------------------------

* Kefalinos Dionysios, 03119030, email?
* Pyliotis Athanasios, 03119050, email?
* Xourdaki Ioanna, 03119203, email?


## Purpose and Goals
----------------------------------------------------------------

blah blah blah blah blah blah blah blah blah

## Infrastructure used
----------------------------------------------------------------

For our infrastructure we have 3 machines with different specifications:

Master: 
* Ubuntu Server LTS 22.04 OS
* 2 Cores (4vCPUs)
* 32GB RAM
* 50GB disk capacity

Worker-1 and worker-2: 
* Ubuntu Server LTS 22.04 OS
* 1 Core (2vCPUs)
* 16GB RAM
* 50GB disk capacity

To set the hosts names we used `sudo vim /etc/hosts` and added the names like below:
```
127.0.0.1       localhost
10.168.0.6   master
10.168.0.7   worker-1
10.168.0.8   worker-2
```
And finally un `sudo reboot` in order for the changes to be set.

