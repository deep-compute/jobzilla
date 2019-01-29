FROM ubuntu:16.04

RUN apt -y update

RUN apt -y upgrade

RUN apt install -y python3.5

RUN apt install -y python3-pip

RUN apt install -y mysql-client

RUN apt-get install -y libmysqlclient-dev

RUN pip3 install kwikapi

RUN pip3 install kwikapi-django

RUN pip3 install mysqlclient

RUN pip3 install basescript

RUN pip3 install docker

RUN pip3 install django-redis==4.9.0

RUN pip3 install python-redis-lock==3.2.0

RUN pip3 install redis==2.10.6

WORKDIR /manager

ADD . /manager

CMD ["python3", "manage.py", "runserver", "0.0.0.0:5678"]
