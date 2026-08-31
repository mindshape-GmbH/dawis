#!/bin/bash

usermod -u ${CELERY_UID} dawis

touch config/delete_me_for_restart
chown dawis:dawis . && chown -R dawis:dawis $(ls -I config)

CELERY_MAX_MEMORY_PER_CHILD=$(($(cat /sys/fs/cgroup/memory.max)/$CELERY_CONCURRENCY/1000))
echo Running workers with $CELERY_MAX_MEMORY_PER_CHILD kB max memory.

/usr/local/bin/python3 -m celery \
    -A dawis \
    worker \
    --uid=${CELERY_UID} \
    --time-limit=${CELERY_TIME_LIMIT} \
    --concurrency=${CELERY_CONCURRENCY} \
    --autoscale=${CELERY_CONCURRENCY},1 \
    --max-memory-per-child=${CELERY_MAX_MEMORY_PER_CHILD} \
    --logfile=${CELERY_LOGFILE_PATH}/worker.log \
    --loglevel=${CELERY_LOGLEVEL} &

workerPid=$!
echo Started worker with pid $workerPid.

/usr/local/bin/python3 -m celery \
    -A dawis \
    beat \
    --uid=${CELERY_UID} \
    --logfile=${CELERY_LOGFILE_PATH}/beat.log \
    --schedule=/opt/dawis/var/beat-schedules/schedule.db \
    --max-interval=60 \
    --loglevel=${CELERY_LOGLEVEL} &

beatPid=$!
echo Started scheduler with pid $beatPid.

trap "echo Stopping... && kill -2 $beatPid && kill -2 $workerPid && tail --pid=$workerPid -f /dev/null && exit" SIGTERM

while true; do sleep 1; done
