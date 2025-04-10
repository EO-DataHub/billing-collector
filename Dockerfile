FROM python:3.11-slim

WORKDIR /app

COPY billing_collector.py .

RUN pip install requests pulsar-client

CMD ["python", "billing_collector.py"]