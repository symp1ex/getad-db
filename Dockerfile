FROM python:3.10-slim

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r/tmp/requirements.txt

COPY templates /app/templates

COPY source /app/source

COPY static /app/static

COPY *.py /app/

WORKDIR /app

CMD ["python", "getad_db.py"]