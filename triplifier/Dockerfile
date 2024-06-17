FROM ubuntu:20.04

WORKDIR /app
COPY . /app

RUN apt-get update -y && \
    apt-get install -y wget && \
    apt-get install -y openjdk-11-jdk && \
	apt-get clean && rm -rf var/lib/apt/lists/* && \
    apt-get install curl -y && \
	apt-get install -y python3 python3-pip python3-dev libpq-dev

RUN pip install -U \
    pip \
    setuptools \
    wheel

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64 #This can vary
ENV PATH $PATH:$JAVA_HOME/bin

ENV POST_URL https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
RUN wget ${POST_URL}

RUN pip3 install -r requirements.txt

CMD ["python3", "/app/data_descriptor/data_descriptor_main.py"]