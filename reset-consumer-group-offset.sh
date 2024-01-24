if [[ "$1" == "" || "$2" == "" ]]
then
  echo "Usage: $0 topic-name consumer-group"	 
  exit 1	
fi

TOPIC=$1
CONSUMER_GROUP=$2

#oc exec my-cluster-kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-group --describe
oc exec my-cluster-kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group $CONSUMER_GROUP --topic $TOPIC --execute --reset-offsets --to-earliest
