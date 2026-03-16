FROM bitnami/spark:3.5.2

USER root
WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt

COPY spark_job.py /app/spark_job.py
COPY schema.json /app/schema.json
COPY config.yml /app/config.yml
COPY slo.yml /app/slo.yml
COPY contracts /app/contracts
COPY ge /app/ge
COPY dataops /app/dataops
COPY lineage /app/lineage

CMD ["/bin/bash", "-lc", "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.microsoft.azure:synapseml_2.12:1.0.8 /app/spark_job.py"]
