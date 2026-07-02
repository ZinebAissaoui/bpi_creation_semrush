FROM python:3.13-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

# Orchestrateur mensuel : sync URLs -> positions M-1/M+1 -> snapshot monthly_run
CMD ["python", "run_pipeline.py"]
