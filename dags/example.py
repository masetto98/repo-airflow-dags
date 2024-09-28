from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Función que imprimirá el mensaje
def hello_world():
    print("Hello, Airflow!")

# Definición de los parámetros básicos del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),  # Fecha de inicio del DAG
    'retries': 1,
}

# Definimos el DAG
with DAG(
    dag_id='test_dag',      # Nombre del DAG
    default_args=default_args,     # Parámetros por defecto
    schedule_interval='@daily',    # Ejecutar el DAG diariamente
    catchup=False,                 # No ejecutar DAGs pasados
) as dag:

    # Definimos la tarea que ejecutará la función hello_world
    hello_task = PythonOperator(
        task_id='say_hello',       # Identificador de la tarea
        python_callable=hello_world,  # Función a ejecutar
    )

# Aquí podemos encadenar más tareas si lo necesitamos
