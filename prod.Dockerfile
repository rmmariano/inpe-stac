# FROM python:3.6.9-alpine
FROM python:3.6.9

RUN mkdir -p /app
WORKDIR /app

COPY . /app
RUN pip install -r requirements.txt

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]
