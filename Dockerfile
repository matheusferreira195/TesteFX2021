FROM jupyter/pyspark-notebook
ADD requirements.txt . 
RUN pip install -r requirements.txt
ADD ./src /src
WORKDIR /src
EXPOSE 8050
CMD ["export SPARK_LOCAL_IP='0.0.0.5'"]
CMD ["python", "analise.py"]