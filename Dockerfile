FROM python:3.10-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY ./requirements.txt /app

RUN pip3 install --upgrade pip
RUN pip3 install gunicorn
RUN pip3 install -r /app/requirements.txt
RUN apt-get update

COPY ./*.py /app/
COPY ./extraction_service /app/
COPY ./mongo_service /app/
COPY ./s3_service /app/
ADD ./.env /app/

EXPOSE 3000

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "server:app", "--bind", "0.0.0.0:3000"] 
