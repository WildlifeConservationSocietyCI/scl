# BASED ON: https://github.com/puckel/docker-airflow
# Possible TODOs:
# - don't remove build deps, use postgres 11 client
# - remove hive,jdbc,mysql,other unnecessary deps?

FROM python:3.7-slim
LABEL maintainer="saanobhaai"

ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.3
ARG SCL_HOME=/var/projects/scl
ENV AIRFLOW_GPL_UNIDECODE yes

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
        wget \
        less \
        nano \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install 'pyOpenSSL>=0.11' \
    && pip install google-api-python-client \
    && pip install oauth2client \
    && pip install earthengine-api \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install apache-airflow[crypto,celery,redis,postgres,hive,jdbc,mysql,ssh]==${AIRFLOW_VERSION} \
    && pip install redis \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY script/entrypoint.sh /entrypoint.sh
EXPOSE 8080 5555 8793

WORKDIR ${SCL_HOME}/airflow
ENTRYPOINT ["/entrypoint.sh"]
# set default arg for entrypoint
CMD ["webserver"]
