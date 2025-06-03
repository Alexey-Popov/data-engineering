
FROM python:3.11

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY etl_script_mysql.py etl_script_mysql.py
COPY data/ data/

CMD ["python", "etl_script_mysql.py"]
