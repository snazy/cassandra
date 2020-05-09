# Dockerfile for building the Cassandra documentation.
# If wanting to regenerate the documentation from scratch,
# run `ant realclean` from the root directory of this project.

FROM python:2.7

WORKDIR /usr/src/code

ARG JVM_VERSION=8

RUN pip install --no-cache-dir sphinx sphinx_rtd_theme

RUN apt-get update && apt-get install -y software-properties-common

RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - \
    && add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ \
    && apt-get update \
    && apt-get install -y adoptopenjdk-${JVM_VERSION}-hotspot ant


RUN apt-get clean

# Do some trickery to run ant using a UID/GID passed in via USE_UID+USE_GID environment variables at runtime.
# There might be easier ways to do it, but the mechanisms provided by Dockerfile + docker-compose are pretty limited.
CMD if [ "x$USE_UID" != "x" ] ; then \
        groupadd --gid $USE_GID work_user ; \
        useradd --create-home --home-dir /usr/src/code/doc/build/docker-home --gid $USE_GID --uid $USE_UID work_user ; \
        su --preserve-environment work_user /bin/sh -c "cd /usr/src/code ; ant gen-doc" ; \
    else \
        ant gen-doc ; \
    fi \
    && echo "The locally built documentation can be found here:\n\n    build/html/index.html\n\n"
