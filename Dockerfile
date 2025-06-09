FROM apache/airflow:2.8.1-python3.10

USER root

# Add backports repo and install OpenJDK 11
RUN apt-get update && \
    apt-get install -y wget gnupg2 software-properties-common && \
    echo "deb http://deb.debian.org/debian bullseye main" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y openjdk-11-jdk build-essential curl netcat-openbsd libpq-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME and update PATH
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Set Spark and Hadoop versions
ENV SPARK_VERSION=3.5.6
ENV HADOOP_VERSION=3

# Install Apache Spark
RUN cd /usr/local && \
    wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Spark env
ENV SPARK_HOME=/usr/local/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
