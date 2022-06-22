# syntax=docker/dockerfile:1

FROM python:3.9-slim-bullseye
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
# APPS PATHS
ENV UX_PATH=$PYLIBS_PATH/ux

# LOG PATHS
#ENV LOGS_PATH=/tmp/tradeexecutor

ENV PYTHONPATH=$PYLIBS_PATH:$HOME

RUN mkdir -p $PYLIBS_PATH
RUN mkdir -p $UX_PATH

#RUN mkdir -p $LOGS_PATH

RUN chown -R $USER $PYLIBS_PATH
RUN chown -R $USER $UX_PATH

#RUN chown -R $USER $LOGS_PATH

WORKDIR $UX_PATH

# Copy pip requirements before copying full project
USER $USER
COPY --chown=$USER:$USER /ux/requirements.txt $UX_PATH

# Install Pip dependencies --> Need to install as root
RUN pip3 install -Ur $UX_PATH/requirements.txt

# Now copy full project
COPY --chown=$USER:$USER /ux $UX_PATH

RUN chmod +x $UX_PATH/run.sh

# Run container as root to be able to create and write in folders
USER root
ENTRYPOINT [ "./run.sh" ]
