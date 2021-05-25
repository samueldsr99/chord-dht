FROM docker.uclv.cu/python:3.9

RUN pip install redis==3.3.11

COPY . /app

WORKDIR /app