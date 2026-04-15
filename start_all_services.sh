#!/bin/bash
echo "Restart service zookeeper và Kafka..."
docker compose restart zookeeper kafka

echo "Khởi động Cluster"
# 1. Khởi động HDFS và YARN
echo "1. Khởi động HDFS (NameNode trên Master, DataNode trên Slave)..."
# 1. Khởi động HDFS và YARN
echo "1. Khởi động HDFS (NameNode trên Master, DataNode trên Slave)..."
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/sbin/start-dfs.sh

echo "2. Khởi động YARN (ResourceManager và NodeManager)..."
echo "2. Khởi động YARN (ResourceManager và NodeManager)..."
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/sbin/start-yarn.sh

# Chờ HDFS lên
echo "Đang chờ HDFS sẵn sàng..."
sleep 15
docker exec 23133083thuyvan-master hdfs dfsadmin -report | head -n 10

echo "Tạo thư mục DFS cho convert từ SparkML sang ONNX..."
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/bin/hadoop fs -mkdir -p /tmp/onnx
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/bin/hadoop fs -chmod -R 777 /tmp

echo "Tạo thư mục lakehouse..."
# Tạo thư mục lakehouse trong HDFS
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/bin/hadoop fs -mkdir -p /user/hive/lakehouse
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/bin/hadoop fs -chmod -R 777 /user

echo "Cấp quyền rwx cho mọi user hiện tại và tương lai trên thư mục lakehouse..."
# Cấp quyền rwx cho mọi user hiện tại và tương lai trên thư mục lakehouse
docker exec 23133083thuyvan-master hdfs dfs -setfacl -m default:user::rwx,default:group::rwx,default:other::rwx /user/hive/lakehouse
# Kiểm tra lại xem đã có các dòng "default" chưa
docker exec 23133083thuyvan-master hdfs dfs -getfacl /user/hive/lakehouse

# . Khởi động Hive Metastore
echo "3. Khởi động Hive Metastore trên Master..."
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hive/bin/hive --service metastore 2>/dev/null || true


echo "TẤT CẢ SERVICE ĐÃ ĐƯỢC KHỞI ĐỘNG"
echo "Kiểm tra DataNode trên Slave:"
docker exec 23133083thuyvan-slave1 jps | grep DataNode
