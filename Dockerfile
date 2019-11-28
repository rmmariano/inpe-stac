FROM python:3.7-slim-buster


RUN apt-get update -y \
    && apt-get install -y libmysqlclient-dev\
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app
WORKDIR /app

COPY requirements.txt /app
RUN pip install -r requirements.txt

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]
