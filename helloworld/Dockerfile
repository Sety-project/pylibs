# syntax=docker/dockerfile:1

FROM python:3.9-slim-bullseye
# FROM ubuntu:focal
RUN apt-get update
# RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y python3 python3-pip
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y python3-pip

# This is for the libreoffice module to run without warnings
# RUN DEBIAN_FRONTEND="noninteractive" apt-get install default-jre-headless -y 
# RUN DEBIAN_FRONTEND="noninteractive" apt-get install libreoffice-java-common -y
# RUN DEBIAN_FRONTEND="noninteractive" apt-get install libreoffice-writer -y

# RUN adduser -u 1026 vic
# RUN usermod -a --group users vic
ENV USER=ec2-user
RUN adduser -u 1026 $USER
RUN usermod -a --group users $USER     # Adding user to group $USER

#Enriching PATH
ENV HOME=/home/$USER
ENV PATH=$PATH:$HOME/.local/bin

ENV PYLIBS_PATH=$HOME/pylibs
# APPS PATHS
ENV HELLOWORLD_PATH=$PYLIBS_PATH/helloworld
# ENV PYTHONPATH=$PYLIBS_PATH:$HOME
ENV PYTHONPATH=$PYLIBS_PATH

RUN mkdir -p $HELLOWORLD_PATH
RUN chown -R $USER $HELLOWORLD_PATH

WORKDIR $HELLOWORLD_PATH

# ENV PYTHONPATH=$PROJECT_PATH:/home/vic
# COPY . --> This is the default pytests root directory. We want to copy only the helloworld project
# COPY --chown=vic:vic ./$PROJECT_NAME/ $ROOT_DIR
# . --> points to the current git context WORKDIR which is where we want to copy the project
# COPY --chown=vic:vic . .
USER $USER
COPY --chown=$USER:$USER /helloworld $HELLOWORLD_PATH

# RUN pip3 install -Ur $PROJECT_PATH/requirements.txt
CMD ["python3", "main.py"]
