# Image for the Python parts of the pipeline: producers, Telegram bot, GTFS loader.
# Used by the "pipeline" profile in docker-compose.yml. The code itself is mounted from the repo
# (see the volumes there), the image only carries Python and the libraries.
FROM python:3.14.7-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# run as an unprivileged user, not root
RUN useradd --create-home app
USER app
