# syntax=docker/dockerfile:1

FROM python:3.10.4-slim-bullseye
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y python3-pip
RUN pip3 install --upgrade pip

ENV USER=ec2-user
RUN adduser -u 1026 $USER
RUN usermod -a --group users $USER     # Adding user to group $USER

#Enriching PATH
ENV HOME=/home/$USER
ENV PATH=$PATH:$HOME/.local/bin

ENV PYLIBS_PATH=$HOME/pylibs
ENV HISTFEED_PATH=$PYLIBS_PATH/histfeed
ENV UTILS_PATH=$PYLIBS_PATH/utils
ENV LOGS_PATH=/tmp/histfeed

RUN mkdir -p $PYLIBS_PATH
RUN mkdir -p $HISTFEED_PATH
RUN mkdir -p $UTILS_PATH
RUN mkdir -p $HOME/mktdata
RUN mkdir -p $LOGS_PATH

RUN chown -R $USER $HOME/mktdata
RUN chown -R $USER $PYLIBS_PATH
RUN chown -R $USER $HISTFEED_PATH
RUN chown -R $USER $UTILS_PATH
RUN chown -R $USER $LOGS_PATH

USER $USER
WORKDIR $HISTFEED_PATH

ENV PYTHONPATH=$PYLIBS_PATH:$HOME

COPY --chown=$USER:$USER /utils $UTILS_PATH
COPY --chown=$USER:$USER /histfeed $HISTFEED_PATH
RUN chmod +x $HISTFEED_PATH/run.sh

# Pip install dependencies --> Need to install as root
USER root
RUN pip3 install -Ur $HISTFEED_PATH/requirements.txt
RUN pip3 install -Ur $UTILS_PATH/requirements.txt

# Run as root
ENTRYPOINT [ "run.sh" ]
