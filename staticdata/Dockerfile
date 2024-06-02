# syntax=docker/dockerfile:1

FROM ubuntu:focal
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y python3 python3-pip

ENV USER=ubuntu
RUN adduser -u 1026 $USER
RUN usermod -a --group users $USER
ENV PYLIBS_PATH=/home/$USER/Sety-project/pylibs
ENV STATICDATA_PATH=$PYLIBS_PATH/staticdata
ENV UTILS_PATH=$PYLIBS_PATH/utils
RUN mkdir -p $PYLIBS_PATH
RUN mkdir -p $STATICDATA_PATH
RUN mkdir -p $UTILS_PATH
RUN mkdir -p /home/$USER/static
RUN chown -R $USER /home/$USER/static
RUN chown -R $USER $PYLIBS_PATH
RUN chown -R $USER $STATICDATA_PATH
RUN chown -R $USER $UTILS_PATH
USER $USER
WORKDIR $STATICDATA_PATH

ENV PYTHONPATH=$PYLIBS_PATH:/home/$USER

COPY --chown=$USER:users /utils $UTILS_PATH
COPY --chown=$USER:users /staticdata $STATICDATA_PATH

RUN pip3 install -Ur $STATICDATA_PATH/requirements.txt

USER $USER
CMD ["python3", "generator/main.py"]
