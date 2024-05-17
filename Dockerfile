FROM python:3.10-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY ./requirements.txt /app

RUN pip3 install --upgrade pip
RUN pip3 install gunicorn
RUN pip3 install -r /app/requirements.txt
RUN apt-get update && \
    apt-get install -y mime-support

COPY ./*.py /app/
COPY ./extraction_service /app/extraction_service
COPY ./mongo_service /app/mongo_service
COPY ./s3_service /app/s3_service
ADD ./.env /app/

EXPOSE 80

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "server:app", "--bind", "0.0.0.0:80"] 
