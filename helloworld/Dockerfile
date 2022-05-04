# syntax=docker/dockerfile:1

FROM ubuntu:focal
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y python3 python3-pip

# This is for the libreoffice module to run without warnings
# RUN DEBIAN_FRONTEND="noninteractive" apt-get install default-jre-headless -y 
# RUN DEBIAN_FRONTEND="noninteractive" apt-get install libreoffice-java-common -y
# RUN DEBIAN_FRONTEND="noninteractive" apt-get install libreoffice-writer -y

RUN adduser -u 1026 vic
RUN usermod -a --group users vic
ENV PROJECT_NAME=helloworld
ENV ROOT_DIR=/home/vic/pytests
ENV PROJECT_PATH=$ROOT_DIR/$PROJECT_NAME
RUN mkdir -p $ROOT_DIR/$PROJECT_NAME
RUN chown -R vic $PROJECT_PATH
USER vic
WORKDIR $PROJECT_PATH

ENV PYTHONPATH=$PROJECT_PATH:/home/vic

# COPY . --> This is the default pytests root directory. We want to copy only the helloworld project
# COPY --chown=vic:vic ./$PROJECT_NAME/ $ROOT_DIR
# . --> points to the current git context WORKDIR which is where we want to copy the project
COPY --chown=vic:vic ./$PROJECT_NAME .

# RUN pip3 install -Ur $PROJECT_PATH/requirements.txt

USER vic
CMD ["python3", "main.py"]
