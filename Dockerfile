FROM apache/airflow:3.1.8

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && JAVA_BIN="$(readlink -f "$(command -v java)")" \
    && JAVA_HOME_DIR="$(dirname "$(dirname "$JAVA_BIN")")" \
    && ln -s "$JAVA_HOME_DIR" /usr/lib/jvm/java-17-openjdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
