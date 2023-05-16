# https://github.com/baosystems/docker-postgis/pkgs/container/postgis
FROM ghcr.io/baosystems/postgis:15

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        postgis \
        time
