FROM maven:3.6.3-openjdk-8-slim  as builder
RUN apt-get update && apt-get install -y curl build-essential procps unzip git \
  python3 python3-pip python3-setuptools python3-cloudpickle python3-requests python3-requests-kerberos python3-flake8 python3-flaky python3-pytest r-base
RUN rm -f /usr/bin/python && ln -s /usr/bin/python3 /usr/bin/python
COPY spark/scripts /scripts
RUN gcc /scripts/watchdir.c -o /scripts/watchdir
RUN chmod +x /scripts/parallel_commands.sh
RUn rm -f /scripts/watchdir.c

# Spark
ENV SPARK_VERSION=3.1.2
ENV SPARK_PACKAGE "spark-${SPARK_VERSION}-bin-hadoop3.2"
ENV SPARK_HOME /usr/spark
ENV PATH="${PATH}:${SPARK_HOME}/bin"
ENV SPARK_CONF_DIR="${SPARK_HOME}/conf"
ENV SPARK_LOG_DIR="${SPARK_HOME}/logs"

RUN curl --progress-bar -L --retry 3 \
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
  | gunzip \
  | tar x -C /usr/ \
 && mv "/usr/${SPARK_PACKAGE}" "${SPARK_HOME}" \
 && chown -R root:root "${SPARK_HOME}" \
 && mkdir $SPARK_LOG_DIR
# For inscrutable reasons, Spark distribution doesn't include spark-hive.jar
# Livy attempts to load it though, and will throw
# java.lang.ClassNotFoundException: org.apache.spark.sql.hive.HiveContext
ARG SCALA_VERSION=2.12
RUN curl --progress-bar -L \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-hive_${SCALA_VERSION}/${SPARK_VERSION}/spark-hive_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    --output "${SPARK_HOME}/jars/spark-hive_${SCALA_VERSION}-${SPARK_VERSION}.jar"
RUN curl --progress-bar -L \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar" \
    --output "${SPARK_HOME}/jars/hadoop-aws-3.2.0.jar"

ENV LIVY_VERSION 0.8.0-incubating-SNAPSHOT
ENV LIVY_PACKAGE apache-livy-$LIVY_VERSION-bin
ENV LIVY_HOME /usr/livy
ENV LIVY_LOGS $LIVY_HOME/logs
ENV LIVY_CONF_DIR $LIVY_HOME/conf
#RUN curl --progress-bar -L --retry 3 \
#    "https://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}/${LIVY_PACKAGE}.zip" >/tmp/livy.zip \
# && unzip /tmp/livy.zip -d /usr/ \
# && mv /usr/$LIVY_PACKAGE $LIVY_HOME \
# && chown -R root:root "${LIVY_HOME}" \
# && rm -f /tmp/livy.zip \
# && mkdir $LIVY_HOME/logs
RUN git clone https://github.com/apache/incubator-livy.git
RUN cd /incubator-livy && mvn package -B -V -e \
                                       -Pspark-3.0 \
                                       -Pthriftserver \
                                       -DskipTests \
                                       -DskipITs \
                                       -Dmaven.javadoc.skip=true
RUN cd /incubator-livy && unzip -qq "./assembly/target/apache-livy-${LIVY_VERSION}-bin.zip" -d /usr \
  && mv "/usr/apache-livy-${LIVY_VERSION}-bin" "${LIVY_HOME}" \
  && rm -rf "./apache-livy-${LIVY_VERSION}-bin.zip" \
  && cd .. \
  && rm -rf "/incubator-livy" \
  && rm -rf "/root/.m2" \
  && mkdir $LIVY_LOGS \
  && chown -R root:root "${LIVY_HOME}"

RUN mkdir /usr/spark/spark-logs

FROM openjdk:8-jdk-slim

ENV SPARK_VERSION=3.1.2
ENV SPARK_PACKAGE "spark-${SPARK_VERSION}-bin-hadoop3.2"
ENV SPARK_HOME /usr/spark
ENV PATH="${PATH}:${SPARK_HOME}/bin"
ENV SPARK_CONF_DIR="${SPARK_HOME}/conf"
ENV SPARK_LOG_DIR="${SPARK_HOME}/logs"
ENV LIVY_VERSION 0.8.0-incubating-SNAPSHOT
ENV LIVY_PACKAGE apache-livy-$LIVY_VERSION-bin
ENV LIVY_HOME /usr/livy
ENV LIVY_LOGS $LIVY_HOME/logs
ENV LIVY_CONF_DIR $LIVY_HOME/conf

COPY --from=builder $SPARK_HOME $SPARK_HOME
COPY --from=builder $LIVY_HOME $LIVY_HOME
COPY --from=builder /scripts /scripts
COPY extralib /extralib

RUN apt-get update && apt-get install -y curl procps unzip \
  python3 python3-pip python3-setuptools python3-cloudpickle \
  python3-requests python3-requests-kerberos python3-flake8 python3-flaky python3-pytest \
  r-base jq

RUN ln -sf /usr/bin/python3 /usr/bin/python

COPY spark/entrypoint.sh /
RUN chmod +x /entrypoint.sh
WORKDIR /usr/spark
ENTRYPOINT ["/entrypoint.sh"]
