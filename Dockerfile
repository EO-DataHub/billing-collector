FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
COPY billing_messager.py .
COPY run.py .

RUN apt-get update && apt-get install -y git && \
	pip install --no-cache-dir -r requirements.txt

CMD ["python", "run.py"]