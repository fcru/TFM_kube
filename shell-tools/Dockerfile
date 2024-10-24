# Use Ubuntu 20.04 (Focal Fossa) as a base image to ensure compatibility
FROM ubuntu:20.04

# Set the environment variable to avoid user interaction during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update the package list and install dependencies
RUN apt-get update && \
    apt-get install -y gnupg wget ca-certificates lsb-release && \
    wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | apt-key add - && \
    echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-6.0.list && \
    apt-get update && \
    apt-get -y install mongodb-org-shell mongodb-mongosh mongodb-org-tools \
                       software-properties-common curl unzip git \
                       joe nano vim default-jre jq less && \
    wget -qO - https://packages.confluent.io/deb/7.1/archive.key | apt-key add - && \
    add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/7.1 stable main" && \
    add-apt-repository "deb https://packages.confluent.io/clients/deb $(lsb_release -cs) main" && \
    apt-get update && apt-get install -y confluent-community-2.13 kafkacat && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt install apt-transport-https && \
    curl -fsSL https://debian.neo4j.com/neotechnology.gpg.key | gpg --dearmor -o /etc/apt/trusted.gpg.d/neo4j.gpg && \
    echo 'deb https://debian.neo4j.com stable latest' | tee -a /etc/apt/sources.list.d/neo4j.list && \
    apt update && \
    apt install -y neo4j

# Install Java 8 for Hadoop
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk

# Set JAVA_HOME for Java 8
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# Install Java 17 for Neo4j and set as default
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

# Set JAVA_HOME for Java 17
ENV JAVA_HOME_17=/usr/lib/jvm/java-17-openjdk-amd64

# Update PATH to include both Java versions
ENV PATH="$JAVA_HOME/bin:$JAVA_HOME_17/bin:$PATH"

# HDFS setup
ENV HADOOP_VERSION 3.3.2
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_COMMON_HOME=${HADOOP_HOME} \
    HADOOP_HDFS_HOME=${HADOOP_HOME} \
    HADOOP_MAPRED_HOME=${HADOOP_HOME} \
    HADOOP_YARN_HOME=${HADOOP_HOME} \
    HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop \
    PATH=${PATH}:${HADOOP_HOME}/bin

# Install Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.2/hadoop-3.3.2.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Set the entrypoint to an indefinite wait command
ENTRYPOINT ["sleep", "infinity"]
