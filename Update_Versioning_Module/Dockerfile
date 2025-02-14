FROM python:3.9

WORKDIR /app

COPY ./requerements.txt /app/requerements.txt
COPY ./API_update_V8.1.py /app

RUN pip install --no-cache-dir -r requerements.txt

ENTRYPOINT [ "python3", "API_update_V8.1.py" ]