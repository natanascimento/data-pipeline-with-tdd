FROM jupyter/pyspark-notebook:spark-3.1.2

USER root

RUN apt update && apt install -y make

RUN pip install pytest 
RUN pip install poetry

COPY ./projects /home/jovyan/work/projects

WORKDIR /home/jovyan/
