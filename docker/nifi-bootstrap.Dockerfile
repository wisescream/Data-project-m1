FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir requests==2.32.3

COPY nifi_bootstrap.py /app/nifi_bootstrap.py

CMD ["python", "/app/nifi_bootstrap.py"]
