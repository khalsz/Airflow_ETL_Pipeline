# use official apache Airlfow image with python 3.10
FROM apache/airflow:latest-python3.10


# set working directory
WORKDIR /opt/airflow

RUN mkdir /opt/airflow/save \
    && chown -R airflow: /opt/airflow/save \
    && chmod +xwr /opt/airflow/save


# copy requirement file to working directory
COPY requirements.txt . 



# installing all dependecies 
RUN pip install --no-cache-dir -r requirements.txt



