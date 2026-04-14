# Usa a imagem oficial do Airflow 3 como base pelo Docker
FROM apache/airflow:3.2.0

# Define o fuso horário no nivel de sistema

ENV TZ=America/Sao_Paulo

# Instala as dependências usando o usuário 'airflow', devido que ao instalar o airflow o próprio airflow cria um usuário com esse nome
# Se não passsar o usuário 'airflow' ele cria no root.
USER airflow

# Copia os arquivos de requisitos para dentro do container

COPY requirements.txt /
COPY requirements-dbt.txt /


RUN pip install --no-cache-dir -r /requirements.txt

# # Cria o ambiente virutal EXCLUSIVO para o dbt 
# RUN bash -c "python -m venv /opt/airflow/.dbt_env && source /opt/airflow/.dbt_env/bin/activate && pip install --no-cache-dir -r /requirements-dbt.txt"
RUN pip install --no-cache-dir -r /requirements-dbt.txt