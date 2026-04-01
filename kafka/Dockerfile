FROM ubuntu:22.04
RUN apt-get update && apt-get install -y openjdk-17-jdk wget sudo net-tools && \
    apt-get clean

RUN useradd -m -s /bin/bash kafka && echo "kafka:kafka" | chpasswd && \
    adduser kafka sudo && echo "kafka ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER kafka
WORKDIR /home/kafka
ENV KAFKA_VERSION=3.9.2
ENV SCALA_VERSION=2.13
RUN wget https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    mv kafka_${SCALA_VERSION}-${KAFKA_VERSION} /home/kafka/kafka && \
    rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz 

ENV KAFKA_HOME=/home/kafka/kafka
ENV PATH=$PATH:$KAFKA_HOME/bin

# Tạo thư mục logs/data cho Kafka 
RUN mkdir -p /home/kafka/kafka/data /home/kafka/kafka/logs && \
    chown -R kafka:kafka /home/kafka/kafka/logs /home/kafka/kafka/data

COPY kafka/server.properties $KAFKA_HOME/config/server.properties

EXPOSE 9092

CMD ["sh", "-c", "$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties"]