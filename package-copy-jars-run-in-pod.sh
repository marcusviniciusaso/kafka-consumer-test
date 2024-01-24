if [[ "$1" == "" || "$2" == "" ]]
then
  echo "Usage: $0 topic-name pod-name"	 
  exit 1	
fi

MVN_DIR="kafka-checker"
TOPIC_NAME=$1
POD_NAME=$2
GROUP_NAME="my-group"

cd $MVN_DIR
mvn clean package
oc cp target/kafka-checker-1.0-SNAPSHOT.jar $POD_NAME:/tmp/
cd ..
oc cp jars/slf4j-api-1.7.36.redhat-00005.jar  $POD_NAME:/tmp/ 
oc cp jars/snappy-java-1.1.10.5-redhat-00001.jar $POD_NAME:/tmp/
oc cp run-in-pod.sh $POD_NAME:/tmp/
oc cp kill-pid-consumer.sh $POD_NAME:/tmp/

oc exec $POD_NAME -- chmod +x /tmp/run-in-pod.sh
oc exec $POD_NAME -- /tmp/run-in-pod.sh
oc exec $POD_NAME -- chmod +x /tmp/kill-pid-consumer.sh
oc exec $POD_NAME -- /tmp/kill-pid-consumer.sh
sleep 2
./reset-consumer-group-offset.sh $TOPIC_NAME $GROUP_NAME
