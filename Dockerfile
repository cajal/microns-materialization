FROM at-docker.ad.bcm.edu:5000/microns-base 
LABEL maintainer="Stelios Papadopoulos <spapadop@bcm.edu>"

RUN pip3 install \
        pcg-skel \
        cloud-volume \
        analysisdatalink\
        caveclient \
        nglui

WORKDIR /root
ARG CLOUDVOLUME_TOKEN
RUN mkdir -p .cloudvolume/secrets
RUN echo "{\"token\": \"${CLOUDVOLUME_TOKEN:-}\"}" > .cloudvolume/secrets/cave-secret.json

COPY . /src/microns-materialization
RUN pip3 install --prefix=$(python -m site --user-base) -e /src/microns-materialization/python/microns-materialization
RUN pip3 install --prefix=$(python -m site --user-base) -e /src/microns-materialization/python/microns-materialization-api