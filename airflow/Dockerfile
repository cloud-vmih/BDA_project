FROM python:3.12-slim

RUN apt-get update && apt-get install -y sudo curl postgresql-client docker.io && rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash airflow && echo "airflow:airflow" | chpasswd && \
    echo "airflow ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

USER airflow
WORKDIR /home/airflow

ENV AIRFLOW_HOME=/home/airflow/airflow
USER ROOT
RUN mkdir -p ${AIRFLOW_HOME} && chown airflow:airflow ${AIRFLOW_HOME}
USER airflow
COPY airflow/requirements.txt ${AIRFLOW_HOME}/requirements.txt

RUN python -m venv ${AIRFLOW_HOME}/venv && \
    ${AIRFLOW_HOME}/venv/bin/pip install --upgrade pip && \
    ${AIRFLOW_HOME}/venv/bin/pip install -r ${AIRFLOW_HOME}/requirements.txt

RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins

COPY airflow/start-airflow.sh /home/airflow/start-airflow.sh
USER root
RUN chmod +x /home/airflow/start-airflow.sh
USER airflow

CMD ["/home/airflow/start-airflow.sh"]
