FROM python:2.7.12

ADD ./spider /work
WORKDIR /work
RUN pip install -r requirements.txt

CMD ["python", "worker.py"]
