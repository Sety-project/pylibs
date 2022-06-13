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
ENV RISKPNL_PATH=$PYLIBS_PATH/riskpnl
ENV UTILS_PATH=$PYLIBS_PATH/utils
ENV LOGS_PATH=/tmp/pnl

ENV PYTHONPATH=$PYLIBS_PATH:$HOME

RUN mkdir -p $PYLIBS_PATH
RUN mkdir -p $UTILS_PATH
RUN mkdir -p $LOGS_PATH

RUN chown -R $USER $PYLIBS_PATH
RUN chown -R $USER $UTILS_PATH
RUN chown -R $USER $LOGS_PATH

WORKDIR $RISKPNL_PATH

# Copy pip requirements before copying full project
USER $USER
COPY --chown=$USER:$USER /riskpnl/requirements.txt $RISKPNL_PATH
COPY --chown=$USER:$USER /utils/requirements.txt $UTILS_PATH

# Install Pip dependencies --> Need to install as root
RUN pip3 install -Ur $RISKPNL_PATH/requirements.txt
RUN pip3 install -Ur $UTILS_PATH/requirements.txt

# Now copy full project
COPY --chown=$USER:$USER /utils $UTILS_PATH
COPY --chown=$USER:$USER /riskpnl $RISKPNL_PATH
RUN chmod +x $RISKPNL_PATH/run.sh

# Run container as root to be able to create and write in folders
USER root
ENTRYPOINT [ "./run.sh" ]