#!/bin/bash
OTHER_DEPENDENCIES="/tmp/slf4j-api-1.7.36.redhat-00005.jar:/tmp/snappy-java-1.1.10.5-redhat-00001.jar"
java -cp /tmp/kafka-checker-1.0-SNAPSHOT.jar:/opt/kafka/libs/kafka-clients-3.6.0.redhat-00005.jar:$OTHER_DEPENDENCIES  com.redhat.Consumer
