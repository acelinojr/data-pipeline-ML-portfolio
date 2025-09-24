# Dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copia o scraper e run_once se quiser
COPY . /app
ENV PYTHONUNBUFFERED=1

CMD ["python", "yahoo_scraper.py"]