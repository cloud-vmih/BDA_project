FROM ubuntu:22.04

# Cài đặt các gói cần thiết
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    openssh-server \
    openssh-client \
    vim \
    nano \
    wget \
    iputils-ping \
    sudo 


# Tạo user và cấu hình SUDO KHÔNG MẬT KHẨU
RUN useradd -m -s /bin/bash hadoop23133083thuyvan && \
    echo "hadoop23133083thuyvan:root" | chpasswd && \
    adduser hadoop23133083thuyvan sudo && \
    echo "hadoop23133083thuyvan ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER hadoop23133083thuyvan
WORKDIR /home/hadoop23133083thuyvan

# Tải và giải nén Hadoop 3.4.1
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz && \
    tar -xzf hadoop-3.4.1.tar.gz && \
    mv hadoop-3.4.1 hadoop && \
    rm hadoop-3.4.1.tar.gz

# Thiết lập biến môi trường
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/home/hadoop23133083thuyvan/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV HADOOP_COMMON_HOME=$HADOOP_HOME
ENV HADOOP_HDFS_HOME=$HADOOP_HOME
ENV HADOOP_YARN_HOME=$HADOOP_HOME
ENV HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
ENV HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

# Cấu hình SSH không mật khẩu
RUN ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "StrictHostKeyChecking no" >> ~/.ssh/config

# Sửa java_home trong hadoop-env.sh
RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh 

# Cài Apache Hive
ENV HIVE_VERSION=3.1.3
COPY apache-hive-3.1.3-bin.tar.gz /home/hadoop23133083thuyvan
RUN tar -xzf apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    mv apache-hive-${HIVE_VERSION}-bin /home/hadoop23133083thuyvan/hive && \
    rm apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    chown -R hadoop23133083thuyvan:hadoop23133083thuyvan /home/hadoop23133083thuyvan/hive

ENV HIVE_HOME=/home/hadoop23133083thuyvan/hive
ENV PATH=$PATH:$HIVE_HOME/bin
ENV HIVE_CONF_DIR=$HIVE_HOME/conf

RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -O $HIVE_HOME/lib/postgresql-42.7.3.jar

# Cài Iceberg
RUN mkdir -p /home/hadoop23133083thuyvan/iceberg && \
    wget https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.1/iceberg-spark-runtime-4.0_2.13-1.10.1.jar \
    -O /home/hadoop23133083thuyvan/iceberg/iceberg-spark-runtime-1.10.1.jar

EXPOSE 9870 8088 9000 22 8032 8042
CMD ["bash", "-c", "sudo service ssh start && tail -f /dev/null"]