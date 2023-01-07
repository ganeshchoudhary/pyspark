FROM apache/airflow:2.1.3-python3.8

USER root

RUN apt-get update

RUN apt-get install git -y

RUN apt-get install vim -y

# RUN cp /etc/timezone /etc/timezone.dist && ln -fs /usr/share/zoneinfo/Asia/Kolkata /etc/localtime
#
# COPY data_pipeline.pth /usr/local/lib/python3.8/site-packages/

USER airflow

# COPY requirements.txt .

# RUN pip install --upgrade pip && pip3 install -r requirements.txt

# COPY airflow.cfg .

RUN mkdir temp

USER root

# COPY data_pipeline dags/data_pipeline
#
# RUN chmod 664 -R dags/data_pipeline
#
# RUN find dags/data_pipeline -type d -exec chmod +x {} \;
#
# COPY data_science_scripts data_science_scripts
#
# RUN chmod 664 -R data_science_scripts

# RUN find data_science_scripts -type d -exec chmod +x {} \;

USER airflow