%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --topic multi-topic --partitions 3 --replication-factor 1 --config segment.bytes=100000