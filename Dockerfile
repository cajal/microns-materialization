FROM ninai/microns-base 
LABEL maintainer="Stelios Papadopoulos <spapadop@bcm.edu>"

RUN pip3 install \
        meshparty \
        cloud-volume \
        analysisdatalink\
        caveclient \
        nglui

WORKDIR /root
ARG CLOUDVOLUME_TOKEN
RUN mkdir -p .cloudvolume/secrets
RUN echo "{\"token\": \"${CLOUDVOLUME_TOKEN:-}\"}" > .cloudvolume/secrets/cave-secret.json

WORKDIR /src
COPY . /src/microns-materialization
RUN pip3 install -e /src/microns-materialization/python/microns-materialization