FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app.py /app/app.py
COPY monitor.py /app/monitor.py
COPY dataops_service.py /app/dataops_service.py
COPY schema.json /app/schema.json
COPY config.yml /app/config.yml
COPY slo.yml /app/slo.yml
COPY contracts /app/contracts
COPY ge /app/ge
COPY dataops /app/dataops
COPY lineage /app/lineage

CMD ["sh", "-c", "mkdir -p /var/log/logstorm && uvicorn app:app --host 0.0.0.0 --port 8000 --no-access-log"]
