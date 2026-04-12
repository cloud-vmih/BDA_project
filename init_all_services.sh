echo "1. Đang bật SSH trên các Node..."
docker exec -u root 23133083thuyvan-master service ssh start
docker exec -u root 23133083thuyvan-slave1 service ssh start

# 2. Xử lý lỗi mất DataNode: Xóa sạch data cũ của HDFS (NameNode + DataNode) để đồng bộ Cluster ID
echo "2. Dọn dẹp HDFS Data cũ và Format NameNode (Ngừa lỗi 'Incompatible clusterIDs')..."
# Chặn các tiến trình NameNode/DataNode/ResourceManager đang kẹt
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/sbin/stop-all.sh 2>/dev/null || true

# Xóa data cũ
docker exec 23133083thuyvan-master bash -c "rm -rf /home/hadoop23133083thuyvan/hadoop/hadoop_data/hdfs/namenode/*"
docker exec 23133083thuyvan-slave1 bash -c "rm -rf /home/hadoop23133083thuyvan/hadoop/hadoop_data/hdfs/datanode/*"

# Sửa lỗi phân quyền nếu có
docker exec -u root 23133083thuyvan-master chown -R hadoop23133083thuyvan:hadoop23133083thuyvan /home/hadoop23133083thuyvan/hadoop/hadoop_data
docker exec -u root 23133083thuyvan-slave1 chown -R hadoop23133083thuyvan:hadoop23133083thuyvan /home/hadoop23133083thuyvan/hadoop/hadoop_data

# Khởi tạo lại NameNode với Cluster ID mới
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/bin/hdfs namenode -format -force

# 3. Khởi tạo Hive Metastore
echo "5. Khởi tạo lakehouse và Hive Metastore..."
# Tạo database PostgreSQL cho metastore (bỏ qua nếu đã có)
docker exec postgres psql -U postgres -c "CREATE DATABASE metastore;" 2>/dev/null || true

# Init Schema Hive
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hive/bin/schematool -dbType postgres -initSchema 2>/dev/null || true

