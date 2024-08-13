FROM openjdk:11-jdk-slim

# Install Python and pip
RUN apt-get update && apt-get install -y python3 python3-pip && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3 1 && \
    update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1

WORKDIR /app

# Set JAVA_HOME
ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "/app/main.py"]
# Or enter the name of your unique directory and parameter set.