sudo docker run -d -p 127.0.0.1:2181:2181 wurstmeister/zookeeper

sudo docker run -d -p 127.0.0.1:9092:9092 \
     -e KAFKA_ADVERTISED_HOST_NAME="127.0.0.1" \
     -e KAFKA_CREATE_TOPICS="blackrock-data:4:1,blackrock-meta:4:1" \
     -e KAFKA_ZOOKEEPER_CONNECT="127.0.0.1:2181" wurstmeister/kafka
