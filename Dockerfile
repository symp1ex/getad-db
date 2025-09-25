FROM python:3.11-slim

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r/tmp/requirements.txt

COPY templates /app/templates

COPY source /app/source

COPY static /app/static

COPY core /app/core

COPY integrations /app/integrations

COPY *.py /app/

WORKDIR /app

CMD ["python", "getad_db.py"]