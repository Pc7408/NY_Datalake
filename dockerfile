from gcr.io/datamechanics/spark:platform-3.1-dm14

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
WORKDIR /opt/application/

COPY Newyorker_Datalake_Solution.py .