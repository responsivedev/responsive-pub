FROM scratch

COPY base/config/docker-compose-base.yml /docker-compose-base.yml
COPY base/config/docker-compose-scylla.yml /docker-compose-scylla.yml
COPY base/config/docker-compose-mongo.yml /docker-compose-mongo.yml
COPY base/config/docker-compose-rs3.yml /docker-compose-rs3.yml
COPY base/config/volumes/scylla.yaml volumes/scylla.yaml
ADD base/config/volumes/zookeeper /volumes/zookeeper
ADD base/config/volumes/scylla /volumes/scylla
ADD base/config/volumes/scylla2 /volumes/scylla2
ADD base/config/volumes/scylla3 /volumes/scylla3
ADD base/config/volumes/kafka /volumes/kafka
ADD base/config/volumes/minio /volumes/minio

COPY async/config/rs3.toml volumes/rs3.toml
COPY async/config/docker-compose.yml /docker-compose.yml
COPY async/config/volumes/app.properties /volumes/app.properties
COPY async/config/volumes/log4j2.properties /volumes/log4j2.properties
