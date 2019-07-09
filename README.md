# SkyTPF Server and Client

This repository contains code and experiments for the ISWC'19 paper "Skyline Queries over Knowledge Graphs". 
This is a **Java** implementation based on Jena. 

## Build
Execute the following command under *server* and *client* directories to build the maven projects and create the executable jar files.
```
$ mvn package
```
At the end of build process, you should have *skytpf-server.jar* file in *server/target* directory and *skytpf-experiments-executor.jar* and *skytpf-client.jar* files in *client/target* directory.

## Deploy stand alone
The server can run with Jetty from a single jar as follows:
```
$ java -jar skytpf-server.jar config.json
```
An example *config.json* file is provided in the *server* directory.

## Experiments
To get the synthetic data used in the paper, the following commands should be used in the directory that contains *skytpf-server.jar*:
```
$ wget http://people.cs.aau.dk/~ilkcan/static/media/skyline_datasets.tar.gz
$ tar -xvzf skyline_datasets.tar.gz 
```
At the end of these commands, a directory containing the hdt files for the dataset named *skyline_datasets* should be created.

In order to get the same results with the experiments presented in the paper, the SkyTPF server should be started with the following command on a virtual machine with 4 2.29 GHZ CPUs and 8GB of main memory to start the SkyTPF server:
```
$ java -Xmx8G -jar skytpf-server.jar config_experiments.json -p 6855
```
*config_experiments.json* file is provided in the *server* directory.

In order to execute the experiments, the following command should be executed:
```
$ java -jar skytpf-experiments-executor.jar distribution numberOfBindings skyTpfServerURL
``` 
**distribution** parameter should be one of the following options: *ACorr* (dataset with anti-correlated distribution), *Corr* (dataset with correlated distribution), *Ind* (dataset with independent distribution). To get results for all the experiments, this command should be executed three times for all possible values of **distribution** paramneter.
**numberOfBindings** parameter is set to 30 for our experiments as suggested by Hartig et al in [the BrTPF paper][https://link.springer.com/chapter/10.1007%2F978-3-319-48472-3_48].
**skyTpfServerURL** should be the address of the SkyTPF server started by the previous command. An example is: *http://127.0.0.1:6855/*

The experiments will provide the same results with the experiments presented in the paper on a virtual machine with 2 2.29 GHZ CPUs and 2GB of main memory.

## Status
SkyTPF currently supports:
- HDT format
- HTML, Turtle, NTriples, JsonLD, RDF/XML output