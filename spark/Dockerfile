FROM ubuntu:22.04

RUN apt-get update && apt-get install -y openjdk-21-jdk wget sudo && apt-get clean
RUN apt-get update && apt-get install -y python3

RUN useradd -m -s /bin/bash spark && echo "spark:spark" | chpasswd && usermod -aG sudo spark && echo 'spark ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/spark && chmod 0440 /etc/sudoers.d/spark

USER spark
WORKDIR /home/spark

ENV SPARK_VERSION=4.0.2
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV SPARK_HOME=/home/spark/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3

COPY master_config/hive-site.xml $SPARK_HOME/conf/

CMD ["/bin/bash", "-c", "\
    if [ \"$SPARK_MODE\" = \"master\" ]; then \
        $SPARK_HOME/sbin/start-master.sh; \
        tail -f /dev/null; \
    else \
        $SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER_URL; \
        tail -f /dev/null; \
    fi"]
