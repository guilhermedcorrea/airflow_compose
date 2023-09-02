
FROM openjdk:8-jdk


ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3


RUN wget https://dlcdn.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3-scala2.13.tgz && \
    tar -xzvf spark-3.4.1-bin-hadoop3-scala2.13.tgz -C /opt/ && \
    mv /opt/spark-3.4.1-bin-hadoop3-scala2.13 $SPARK_HOME && \
    rm spark-3.4.1-bin-hadoop3-scala2.13.tgz


EXPOSE 4040

