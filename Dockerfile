FROM python:3.9-slim

ARG DOCKER_USER=dawis

RUN apt update && apt install -y --no-install-recommends \
    neovim \
    procps \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install pipenv

RUN echo "ls -lah \$@" > /usr/bin/ll && chmod 777 /usr/bin/ll

RUN addgroup --gid 1000 ${DOCKER_USER} && \
    adduser --uid 1000 --gid 1000 --shell /bin/sh --disabled-password --quiet ${DOCKER_USER}

COPY src /app

WORKDIR /app

RUN pipenv install --system

ENV CELERY_UID=1000
ENV CELERY_BEAT_SCHEDULEFILE_PATH="/opt/dawis/var/beat-schedules"
ENV CELERY_LOGFILE_PATH="/opt/dawis/logs"
ENV CELERY_LOGLEVEL="info"
ENV CELERY_TIME_LIMIT=600
ENV CELERY_CONCURRENCY=4
ENV CELERY_TIMEZONE="Europe/Berlin"
ENV CELERY_BROKER_URL="redis://redis_host:6379"

COPY --chmod=775 run.sh /usr/local/bin/
CMD ["run.sh"]
