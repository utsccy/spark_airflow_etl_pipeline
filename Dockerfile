FROM apache/airflow:2.5.0-python3.8

USER root

# Install OpenJDK
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Install project requirements
COPY --chown=airflow:0 requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt