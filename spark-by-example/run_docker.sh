#!/bin/bash

docker run -d -p 8888:8888 -p 4040:4040 -p 4041:4041 -v $PWD:/home/jovyan/work -v $PWD/../LearningSparkV2:/home/jovyan/LearningSparkV2 jupyter/pyspark-notebook
