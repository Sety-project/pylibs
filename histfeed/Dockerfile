# syntax=docker/dockerfile:1

FROM ubuntu:focal
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y python3 python3-pip

ENV USER=ec2-user
RUN adduser -u 1026 $USER
RUN usermod -a --group users $USER
ENV PYLIBS_PATH=/home/$USER/pylibs
ENV HISTFEED_PATH=$PYLIBS_PATH/histfeed
ENV UTILS_PATH=$PYLIBS_PATH/utils
ENV LOGS_PATH=/tmp/histfeed

RUN mkdir -p $PYLIBS_PATH
RUN mkdir -p $HISTFEED_PATH
RUN mkdir -p $UTILS_PATH
RUN mkdir -p /home/$USER/mktdata
RUN mkdir -p $LOGS_PATH

RUN chown -R $USER /home/$USER/mktdata
RUN chown -R $USER $PYLIBS_PATH
RUN chown -R $USER $HISTFEED_PATH
RUN chown -R $USER $UTILS_PATH
RUN chown -R $USER $LOGS_PATH

USER $USER
WORKDIR $HISTFEED_PATH

ENV PYTHONPATH=$PYLIBS_PATH:/home/$USER

COPY --chown=$USER:$USER /utils $UTILS_PATH
COPY --chown=$USER:$USER /histfeed $HISTFEED_PATH

RUN pip3 install -Ur $HISTFEED_PATH/requirements.txt
RUN pip3 install -Ur $UTILS_PATH/requirements.txt

USER $USER
CMD ["python3", "main.py"]
