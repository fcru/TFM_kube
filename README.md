# Pre requisites
- Install mvn `brew install mvn`
- Install k9s: https://k9scli.io/topics/install/
- Install minikube: https://minikube.sigs.k8s.io/docs/start/?arch=%2Fmacos%2Farm64%2Fstable%2Fbinary+download#Service

# Start minikube
```
minikube --memory 5939 --cpus 3 --driver=docker --kubernetes-version=v1.24.0 start
minikube addons enable ingress
minikube tunnel
```

# Interactive shell for a Running pod
```
kubectl exec -it <pod-name> -- /bin/bash
```

# Install argocd
```
kubectl create namespace argocd
kubectl apply -n argocd -f manifests/install-argocd.yaml
```
Visit argocd UI at: localhost/argo-cd

# Install Hadoop
```
kubectl apply -f manifests/install-hadoop.yaml
kubectl apply -f manifests/ingress-hdfs.yaml
kubectl apply -f manifests/ingress-yarn.yaml
```
After tunnel activation HDFS UI and yarn UI could be visited respectively to hdfs.lvh.me and yarn.lvh.me

# Install Mongodb
```
kubectl apply -f manifests/install-mongodb.yaml
```

### Create database

In the cronjob-shell-tools shell: 

Connect to mongodb with `mongosh mongodb://mongodb:27017`

```
use testdb
db.createCollection("uuids")
show dbs
show collections
```

### Show uuids content
```
db.uuids.find().pretty()
```

# Install Kafka
```
kubectl apply -f manifests/install-kafka.yaml
```

## Configuration Files
#### client.properties
```
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user1" password="XXXXX";
```

#### jaas.conf
```
KafkaClient {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="user1"
  password="XXXXX";
};
```

## Create a topic
```
KAFKA_OPTS="-Djava.security.auth.login.config=jaas.conf" kafka-topics --create --topic my-topic --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --command-config client.properties
```

## Produce Events
```
kafka-console-producer --producer.config client.properties --broker-list kafka:9092 --topic my-topic --property "parse.key=true" --property "key.separator=:"
```

## Consume Events
```
kafka-console-consumer --consumer.config client.properties --bootstrap-server kafka:9092 --topic my-topic --from-beginning --property print.key=true --property key.separator=" : "
```

# Install Neo4j
```
kubectl apply -f manifests/install-neo4j.yaml
```

### Connect to neo4J
```
cypher-shell -a neo4j://neo4j-neo4j:7687
```

* [Cypher Documentation](https://neo4j.com/docs/operations-manual/current/tools/cypher-shell/)

# Build & Run docker image
```
./cicd.sh
kubectl apply -f manifests/cronjob-spark.yaml
```
