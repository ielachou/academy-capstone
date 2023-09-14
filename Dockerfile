FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

COPY requirements.txt .

USER root

RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

ENTRYPOINT ["python3"]
CMD ["src/main.py"]