FROM apache/airflow:3.1.7

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends docker.io gosu && \
    rm -rf /var/lib/apt/lists/*

COPY --chmod=755 airflow/entrypoint.sh /entrypoint.sh

USER root
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint.sh"]