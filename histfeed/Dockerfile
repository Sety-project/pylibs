# syntax=docker/dockerfile:1

FROM python:3.10.4-slim-bullseye
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y python3-pip
RUN pip3 install --upgrade pip

ENV USER=ubuntu
RUN adduser -u 1026 $USER
RUN usermod -a --group users $USER     # Adding user to group $USER

#Enriching PATH
ENV HOME=/home/$USER/Sety-project
ENV PATH=$PATH:/home/$USER/.local/bin

ENV PYLIBS_PATH=$HOME/pylibs
ENV HISTFEED_PATH=$PYLIBS_PATH/histfeed
ENV UTILS_PATH=$PYLIBS_PATH/utils
ENV TRADEEXECUTOR_PATH=$PYLIBS_PATH/tradeexecutor
ENV LOGS_PATH=/tmp/histfeed

RUN mkdir -p $PYLIBS_PATH
RUN mkdir -p $HISTFEED_PATH
RUN mkdir -p $UTILS_PATH
RUN mkdir -p $TRADEEXECUTOR_PATH
RUN mkdir -p $HOME/mktdata
RUN mkdir -p $HOME/config
RUN mkdir -p $LOGS_PATH

RUN chown -R $USER $PYLIBS_PATH
RUN chown -R $USER $HISTFEED_PATH
RUN chown -R $USER $UTILS_PATH
RUN chown -R $USER $TRADEEXECUTOR_PATH
RUN chown -R $USER $HOME/mktdata
RUN chown -R $USER $HOME/config
RUN chown -R $USER $LOGS_PATH

WORKDIR $HISTFEED_PATH

# Copy pip requirements before copying full project
USER $USER
COPY --chown=$USER:$USER /requirements.txt $UTILS_PATH

# Install Pip dependencies --> Need to install as root
RUN pip3 install -Ur $UTILS_PATH/requirements.txt

# Now copy full project
COPY --chown=$USER:$USER /utils $UTILS_PATH
COPY --chown=$USER:$USER /histfeed $HISTFEED_PATH
COPY --chown=$USER:$USER /tradeexecutor $TRADEEXECUTOR_PATH
RUN chmod +x $HISTFEED_PATH/run.sh

# Run container as root to be able to create and write in folders
USER root
ENTRYPOINT [ "./run.sh" ]
