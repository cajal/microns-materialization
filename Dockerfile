FROM at-docker:5000/microns-base:cuda11.8.0-python3.8
LABEL maintainer="Stelios Papadopoulos <spapadop@bcm.edu>"

RUN pip3 install \
        pcg-skel \
        cloud-volume \
        analysisdatalink\
        caveclient \
        nglui

WORKDIR /
ARG CLOUDVOLUME_TOKEN
RUN mkdir -p .cloudvolume/secrets
RUN echo "{\"token\": \"${CLOUDVOLUME_TOKEN:-}\"}" > .cloudvolume/secrets/cave-secret.json

COPY . /src/microns-materialization
RUN pip install -e /src/microns-materialization/python/microns-materialization
RUN pip install -e /src/microns-materialization/python/microns-materialization-api