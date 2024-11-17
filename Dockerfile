FROM python:3.9-slim

WORKDIR /app

# Install required dependencies
COPY scripts/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts to container
COPY scripts /app/scripts

CMD ["python", "scripts/main.py"]

