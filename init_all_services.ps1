Write-Host "1. Đang bật SSH trên các Node..."
docker exec -u root 23133083thuyvan-master service ssh start
docker exec -u root 23133083thuyvan-slave1 service ssh start

# 2. Dọn dẹp HDFS và format lại NameNode
Write-Host "2. Dọn dẹp HDFS Data cũ và Format NameNode..."

# Stop service (bỏ qua lỗi nếu có)
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/sbin/stop-all.sh 2>$null

# Xóa data cũ
docker exec 23133083thuyvan-master bash -c "rm -rf /home/hadoop23133083thuyvan/hadoop/hadoop_data/hdfs/namenode/*"
docker exec 23133083thuyvan-slave1 bash -c "rm -rf /home/hadoop23133083thuyvan/hadoop/hadoop_data/hdfs/datanode/*"

# Fix permission
Write-Host "Fix quyền thư mục Hadoop..."
docker exec -u root 23133083thuyvan-master chown -R hadoop23133083thuyvan:hadoop23133083thuyvan /home/hadoop23133083thuyvan/hadoop/hadoop_data
docker exec -u root 23133083thuyvan-slave1 chown -R hadoop23133083thuyvan:hadoop23133083thuyvan /home/hadoop23133083thuyvan/hadoop/hadoop_data

# Format NameNode
Write-Host "Format NameNode..."
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/bin/hdfs namenode -format -force

# 3. Khởi tạo Hive Metastore
Write-Host "3. Khởi tạo lakehouse và Hive Metastore..."

# Tạo database (bỏ qua nếu đã tồn tại)
docker exec postgres psql -U postgres -c "CREATE DATABASE metastore;" 2>$null

# Init schema Hive
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hive/bin/schematool -dbType postgres -initSchema 2>$null

Write-Host "=== HOÀN THÀNH INIT CLUSTER ==="