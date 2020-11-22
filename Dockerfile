FROM python:3.8-buster

RUN apt-get update
RUN apt-get install -y tdsodbc unixodbc-dev
RUN apt install unixodbc-bin -y
RUN apt-get clean -y

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

RUN mkdir -p /src
COPY lime_etl /src/
COPY tests/ /tests/

WORKDIR /src
CMD python -m src.main
