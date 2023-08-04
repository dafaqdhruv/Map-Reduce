# Map-Reduce

An implementation of the [MapReduce model](http://research.google.com/archive/mapreduce-osdi04.pdf) in Golang.  
The project is an attempt on [Lab1](http://nil.csail.mit.edu/6.824/2021/labs/lab-mr.html) of 6.824 Distributed Systems by MIT.

Any MapReduce application comprises of two basic components :

* `Worker` : Component that sub-tasks are assigned to

* `Master` : Component responsible for managing all the workers and ensures completion of assigned task

This repository provides a CLI tool `mapReducer` to launch these components as required.


## Installation

* Clone the repository

    ```
    git clone git@github.com:dafaqdhruv/Map-Reduce.git
    ```

* Build `mapReducer` for the required task

    ```
    make build plugin=plugin/xyz.go
    ```

    where `plugin/xyz.go` is the file containing required `map()` and `reduce()` methods for the task.

## Usage

* Launch task coordinator with inputs files

    ```
    ./mapReducer mrcoordinator [input-files...]
    ```

* Launch a single worker with plugin

    ```
    ./mapReducer mrworkers xyz.so
    ```

* Launch multiple workers

    ```
    ./mapReducer mrworkers xyz.so &
    ./mapReducer mrworkers xyz.so &
    ./mapReducer mrworkers xyz.so
    ```

