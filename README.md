# Skyline Queries over Knowledge Graphs
This repository contains code and experiments for the ISWC'19 paper "Skyline Queries over Knowledge Graphs". 

## System Requirements
In order to be able to build SkyTPF, you need to have the following software installed:\
- mvn: We tested with the version 3.5.4,\
- java: version 8 at least.
- hdt-jena: SkyTPF requires the 2.1-SNAPSHOT version of hdt-jena library and unfortunately, this library is not part of the Maven central repository. For this reason, you need to install this maven dependency to your local maven repository. In order to do that you should execute the following commands:

```
$ wget https://github.com/rdfhdt/hdt-java/archive/master.zip
$ unzip master.zip
$ cd hdt-java-master
$ mvn install
```

If you want to run all the experiments in the paper, you need to have access to two virtual machines (vms): *server vm* for deploying the server, and *client vm* for running the experiments. The server vm used in the experiments has 4 2.29 GHZ CPUs and 8GB of main memory. The client vm used has 2 2.29 GHZ CPUs and 2GB of main memory. Please note that the experimental results might be different when the experiments are completed on machines with different configurations but we expect that the relative performances of the algorithms will remain the same.

## Build
In order to able to reproduce our experiments, you need to build server and client projects first.

### Server
You should first copy the *server* directory to the server vm. (From now on, we assume the server directory is located under the home folder ~). 

You need to run the following commands to build the server.

```
$ cd ~/server/  # cd to server directory
$ mvn package
```
At the end of build process, you should have `skytpf-server.jar` file in `~/server/target` directory.

### Client 
You should first copy the *client* directory to the client vm. (From now on, we assume the client directory is located under the home folder ~).

You need to run the following commands to build the client.
```
$ cd ~/client/  # cd to client directory
$ mvn package
```
At the end of build process, you should have `skytpf-experiments-executor.jar` and `skytpf-client.jar` files in `~/client/target` directory.

## Starting the Server
After build step is completed, you need to start the server that will be used for the experiments. 

In order to start the server, you first need to download the datasets that are used in the experiments. 
```
$ cd ~
$ wget http://people.cs.aau.dk/~ilkcan/static/media/skyline_datasets.tar.gz
$ tar -xvzf skyline_datasets.tar.gz 
```
At the end of these commands, you will have a directory named `skyline_datasets` under your home directory (~).

After downloading the datasets, you need to copy the `skytpf-server.jar` and the config file `config_experiments.json` to the home directory since all these files have to be in the same directory.
```
$ cd ~
$ cp ~/server/config_experiments.json .
$ cp ~/server/target/skytpf-server.jar .
```
Now, you are ready to start the server executing the command given below in your home folder under the server vm. Note that we are starting the server on port 6855. The config file that is used in the experiments `config_experiments.json` is provided in the *server* directory. It simply includes the information regarding prefixes, and the datasets.
```
$ java -Xmx8G -jar skytpf-server.jar config_experiments.json -p 6855
```


## Experiments
After you start the server, you need to execute the experiments in the *client vm*.

In order to execute the experiments, the following commands should be executed:
```
$ cd ~/client/target
$ java -jar skytpf-experiments-executor.jar distribution numberOfBindings skyTpfServerURL
``` 
**distribution** parameter should be one of the following options: *ACorr* (dataset with anti-correlated distribution), *Corr* (dataset with correlated distribution), *Ind* (dataset with independent distribution). To get results for all the experiments, this command should be executed three times for all possible values of **distribution** parameter.
**numberOfBindings** parameter is set to 30 for our experiments as suggested by Hartig et al in [the BrTPF paper](https://link.springer.com/chapter/10.1007%2F978-3-319-48472-3_48).
**skyTpfServerURL** should be the address of the SkyTPF server started by the previous command. An example is: *http://127.0.0.1:6855/*

The experiments will provide the same results with the experiments presented in the paper on a virtual machine with 2 2.29 GHZ CPUs and 2GB of main memory.

At the end of experiments, a CSV file will be created with the following columns:\
*dist*: The distribution parameter,\
*nd*: Number of dimensions,\
*ne*: Number of entities,\
*stmt*: Query processing time for SkyTPF multi-threaded version,\
*stmtnc*: Number of skyline candidates for SkyTPF multi-threaded version, \
*stmtnr*: Number of HTTP requests for SkyTPF multi-threaded version,\
*stst*: Query processing time for SkyTPF single-threaded version,\
*ststnc*: Number of skyline candidates for SkyTPF single-threaded version,\
*ststnr*: Number of HTTP requests for SkyTPF single-threaded version,\
*brtmt*: Query processing time for brTPF multi-threaded version,\
*brtmtnc*: Number of skyline candidates for brTPF multi-threaded version,\
*brtmtnr*: Number of HTTP requests for brTPF multi-threaded version,\
*brtst*: Query processing time for brTPF single-threaded version,\
*brtstnc*: Number of skyline candidates for brTPF single-threaded version,\
*brtstnr*: Number of HTTP requests for brTPF single-threaded version,\
*tpfmt*: Query processing time for TPF multi-threaded version,\
*tpfmtnc*: Number of skyline candidates for TPF multi-threaded version,\
*tpfmtnr*: Number of HTTP requests for TPF multi-threaded version,\
*tpfst*: Query processing time for TPF single-threaded version,\
*tpfstnc*: Number of skyline candidates for TPF single-threaded version,\
*tpfstnr*: Number of HTTP requests for TPF single-threaded version.

## Status
SkyTPF currently supports:
- HDT format
- HTML, Turtle, NTriples, JsonLD, RDF/XML output
