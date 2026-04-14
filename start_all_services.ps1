Write-Host "=== Script Khởi động Cluster An Toàn ==="

# 1. Khởi động HDFS và YARN
Write-Host "1. Khởi động HDFS (NameNode trên Master, DataNode trên Slave)..."
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/sbin/start-dfs.sh

Write-Host "2. Khởi động YARN (ResourceManager và NodeManager)..."
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/sbin/start-yarn.sh

# Chờ HDFS lên
Write-Host "Đang chờ HDFS sẵn sàng..."
Start-Sleep -Seconds 15
docker exec 23133083thuyvan-master bash -c "hdfs dfsadmin -report | head -n 10"

# Tạo thư mục lakehouse trong HDFS
Write-Host "Tạo thư mục lakehouse trong HDFS..."
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/bin/hadoop fs -mkdir -p /user/hive/lakehouse
docker exec 23133083thuyvan-master /home/hadoop23133083thuyvan/hadoop/bin/hadoop fs -chmod -R 777 /user

# Cấp quyền ACL
Write-Host "Thiết lập quyền ACL..."
docker exec 23133083thuyvan-master hdfs dfs -setfacl -m default:user::rwx,default:group::rwx,default:other::rwx /user/hive

# Kiểm tra ACL
docker exec 23133083thuyvan-master hdfs dfs -getfacl /user/hive

# Khởi động Hive Metastore
Write-Host "3. Khởi động Hive Metastore trên Master..."
docker exec 23133083thuyvan-master bash -c "/home/hadoop23133083thuyvan/hive/bin/hive --service metastore" 2>$null

Write-Host "TẤT CẢ SERVICE ĐÃ ĐƯỢC KHỞI ĐỘNG"
Write-Host "Kiểm tra DataNode trên Slave:"
docker exec 23133083thuyvan-slave1 jps | findstr DataNode