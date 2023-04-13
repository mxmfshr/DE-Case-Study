FROM python:3-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc cron \
    && pip install psycopg2 pymysql sqlalchemy pandas cryptography
WORKDIR /app
COPY . .
RUN crontab crontab
CMD ["/bin/bash", "-c", "printenv > /etc/environment && cron -f"] 