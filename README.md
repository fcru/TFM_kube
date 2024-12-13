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
kubectl exec -it shell-tools-cronjob-manual-s8w-q96dv -- /bin/bash
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

## Create directory

In the cronjob-shell-tools shell: 
```
hdfs dfs -mkdir -p hdfs://hadooop-hadoop-hdfs-nn:9000/test/set
hdfs dfs -ls hdfs://hadooop-hadoop-hdfs-nn:9000/test/
```

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


# Install Kafka
```
kubectl apply -f manifests/install-kafka.yaml
```

## Configuration Files

In the cronjob-shell-tools shell:

#### client.properties
```
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user1" password="XXXXX";
```
Jaas.conf is no longer necessary to create as it has been created within the project and is saved via the dockerfile.
It is only necessary to create it before creating the kafka topics

To change the kafka secret, modify the file kafka_client_jaas.conf.

#### jaas.conf
```
KafkaClient {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="user1"
  password="XXXXX";
};
```

## Create a topic
In the cronjob-shell-tools shell:
```
KAFKA_OPTS="-Djava.security.auth.login.config=jaas.conf" kafka-topics --create --topic truck-route --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --command-config client.properties
```
## Delete a topic
In the cronjob-shell-tools shell:
```
KAFKA_OPTS="-Djava.security.auth.login.config=jaas.conf" kafka-topics --delete --topic truck-route --bootstrap-server kafka:9092 --command-config client.properties


```

## Produce Events
```
kafka-console-producer --producer.config client.properties --broker-list kafka:9092 --topic my-topic --property "parse.key=true" --property "key.separator=:"
```

## Consume Events
```
kafka-console-consumer --consumer.config client.properties --bootstrap-server kafka:9092 --topic truck-route --from-beginning --property print.key=true --property key.separator=" : "
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

# Execute streaming and batch pipelines

## Streaming processing

### Create topics
In the cronjob-shell-tools shell:

Create client.properties
```
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user1" password="XXXXX";
```
Create topics
```
KAFKA_OPTS="-Djava.security.auth.login.config=jaas.conf" kafka-topics --create --topic estat_estacions --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
KAFKA_OPTS="-Djava.security.auth.login.config=jaas.conf" kafka-topics --create --topic truck-route --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```

### Create the cronjobs for each of the processes
To run the streaming pipeline, the cronjobs of each of the following processes must be executed:

- 1_data_ingestion
- 2_stations_clustering
- 3_update_Stations_status
- 4_optimal_route

In each folder, run:
```
./build.sh
```
With this, the dockerfile and the cronjob are executed simultaneously.

Once the cronjobs are created, if the cronjob configuration is `suspend = false`, a trigger will be made automatically in k9s. 

Otherwise, if `suspend = true` a manual trigger will have to be done on the k9s

## Batch processing

### Expansion Strategy analysis

To execute the expansion strategy analysis you just need to run the cronjobs of any zone in sequential order:

- Landing Zone
- Trusted Zone
- Exploitation Zone
- Geojson analysis
- Visualization

For each zone you should build & run the docker image with the shown command. 
As an example for the landing zone:
```
./build_expansion_landing.sh
kubectl apply -f cronjob-expnsion-landing.yaml
```
Once created the cronjob you have to trigger it using k9s. The cronjob will automatically execute the code. 

The visualization part expose a streamlit app on the port 8501 so you should expose the pod containing it directly in k9s and visit the app on localhost:8501.

# Metrics

In manifests folder:
```
kubectl applt -f prometheus.yaml
kubectl applt -f prometheus-ConfigMap.yaml
kubectl applt -f prometheus-service.yaml
kubectl applt -f pushgateway.yaml
kubectl applt -f install-grafana.yaml
```

In 3_update_stations_status folder of streaming pipeline:
```
kubectl apply -f metrics_service.yaml
```
If you want to see the metrics collected by **Prometheus**:

On k9s, run a prometheus services port-forward on port 9090.

Open in browser:
```
http://localhost:9090
```

If you want to see the metrics in **Grafana**:

On k9s, run a grafana services port-forward on port 3000.

Open in browser:
```
http://localhost:3000/
```
In the Grafana data source configuration, indicate that the data comes from the service:
```
http://prometheus-service:9090
```

If you want to see the **Kafka** metrics collected by **JMX Exporter**:

```
kubectl port-forward svc/kafka-jmx-metrics 5556:5556
```



