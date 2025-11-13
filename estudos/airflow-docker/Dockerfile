# ----------------------------------------------------
# 1. ESTÁGIO DE BUILD (Temporário): Baixar o Spark do Bitnami
# ----------------------------------------------------
FROM apache/spark:3.5.1 AS spark_builder 

# <-- ALTERADO DE '3.5.1-debian-11-r0' para '3.5.1'

# ----------------------------------------------------
# 2. ESTÁGIO FINAL: Airflow
# ----------------------------------------------------
FROM apache/airflow:3.1.1 

# Instala o pacote de provedor do Spark
RUN pip install apache-airflow-providers-apache-spark

# ----------------------------------------------------
# Copiar o Spark do estágio temporário
# ----------------------------------------------------
ENV SPARK_HOME=/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

# Copia os binários e libs do Spark da imagem Apache Spark
COPY --from=spark_builder /opt/spark $SPARK_HOME