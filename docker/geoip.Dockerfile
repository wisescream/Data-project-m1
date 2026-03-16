FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi==0.115.12 uvicorn[standard]==0.34.0 boto3==1.37.14

COPY geoip_service.py /app/geoip_service.py

CMD ["uvicorn", "geoip_service:app", "--host", "0.0.0.0", "--port", "8081"]
