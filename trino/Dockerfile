FROM trinodb/trino:latest

USER root

# Hadoop config
COPY master_config/core-site.xml /etc/trino/core-site.xml
COPY master_config/hdfs-site.xml /etc/trino/hdfs-site.xml

# Iceberg config
RUN mkdir -p /etc/trino/catalog
COPY trino/catalog/iceberg.properties /etc/trino/catalog/iceberg.properties

EXPOSE 8080
CMD ["/usr/lib/trino/bin/run-trino"]