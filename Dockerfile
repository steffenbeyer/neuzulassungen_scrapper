FROM python:3.12-slim

WORKDIR /app

# System-Dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Python-Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Anwendung kopieren
COPY . .

# Datenverzeichnis
RUN mkdir -p /app/data/raw

# Cron-Job fuer taegliche Pruefung auf neue Daten
RUN echo "0 8 * * * cd /app && python main.py --mode update >> /var/log/cron.log 2>&1" > /etc/cron.d/scrapper-cron \
    && chmod 0644 /etc/cron.d/scrapper-cron \
    && crontab /etc/cron.d/scrapper-cron

# Standard: Initialer Import
CMD ["python", "main.py", "--mode", "initial"]
