# syntax=docker/dockerfile:1

FROM python:3.9-slim-bullseye
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y python3-pip
RUN pip3 install --upgrade pip

ENV USER=ubuntu
RUN adduser -u 1026 $USER
RUN usermod -a --group users $USER     # Adding user to group $USER

#Enriching PATH
ENV HOME=/home/$USER
ENV PATH=$PATH:$HOME/.local/bin

ENV PYLIBS_PATH=$HOME/Sety-project/pylibs
# APPS PATHS
ENV HISTFEED_PATH=$PYLIBS_PATH/histfeed
ENV PFOPTIMIZER_PATH=$PYLIBS_PATH/pfoptimizer
ENV RISKPNL_PATH=$PYLIBS_PATH/riskpnl
ENV UTILS_PATH=$PYLIBS_PATH/utils
ENV TRADEEXECUTOR_PATH=$PYLIBS_PATH/tradeexecutor
# LOG PATHS
ENV LOGS_PATH=/tmp/pfoptimizer

RUN mkdir -p $PYLIBS_PATH
RUN mkdir -p $HISTFEED_PATH
RUN mkdir -p $PFOPTIMIZER_PATH
RUN mkdir -p $RISKPNL_PATH
RUN mkdir -p $UTILS_PATH
RUN mkdir -p $TRADEEXECUTOR_PATH
RUN mkdir -p $HOME/Sety-project/mktdata
RUN mkdir -p $HOME/Sety-project/config
RUN mkdir -p $LOGS_PATH

RUN chown -R $USER $PYLIBS_PATH
RUN chown -R $USER $HISTFEED_PATH
RUN chown -R $USER $PFOPTIMIZER_PATH
RUN chown -R $USER $RISKPNL_PATH
RUN chown -R $USER $UTILS_PATH
RUN chown -R $USER $TRADEEXECUTOR_PATH
RUN chown -R $USER $HOME/Sety-project/mktdata
RUN chown -R $USER $HOME/Sety-project/config
RUN chown -R $USER $LOGS_PATH

WORKDIR $PFOPTIMIZER_PATH

# Copy pip requirements before copying full project
USER $USER
COPY --chown=$USER:users /requirements.txt $PYLIBS_PATH

# Install Pip dependencies --> Need to install as root
RUN pip3 install -Ur $PYLIBS_PATH/requirements.txt

# Now copy full project
COPY --chown=$USER:users /pfoptimizer $PFOPTIMIZER_PATH
COPY --chown=$USER:users /histfeed $HISTFEED_PATH
COPY --chown=$USER:users /riskpnl $RISKPNL_PATH
COPY --chown=$USER:users /utils $UTILS_PATH
COPY --chown=$USER:users /tradeexecutor $TRADEEXECUTOR_PATH
RUN chmod +x $PFOPTIMIZER_PATH/run.sh

# Run container as root to be able to create and write in folders
USER root
ENTRYPOINT [ "./run.sh" ]
