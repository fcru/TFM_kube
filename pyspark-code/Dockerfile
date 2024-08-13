FROM spark:3.4.1-scala2.12-java11-python3-ubuntu

USER root

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

#COPY ./src/main/example/spark-job.py /opt/spark-job.py
COPY ./src/main/example/mongo-job.py /opt/mongo-job.py

# Set the working directory
WORKDIR /opt

# Set the entrypoint to run specific test:

#ENTRYPOINT ["/opt/spark/bin/spark-submit", "/opt/spark-job.py"]
ENTRYPOINT ["/opt/spark/bin/spark-submit", "/opt/mongo-job.py"]