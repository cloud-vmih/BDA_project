docker exec -it 23133083thuyvan-master bash
hdfs namenode -format
start-all.sh
jps
docker exec -it 23133083thuyvan-slave1 bash

# Có lỗi slave thiếu datanode thì vào master chạy lệnh này rồi format lại
rm -rf /home/hadoop23133083thuyvan/hadoop/hadoop_data/hdfs/namenode/*
ssh 23133083thuyvan-slave1 "rm -rf /home/hadoop23133083thuyvan/hadoop/hadoop_data/hdfs/datanode/*"