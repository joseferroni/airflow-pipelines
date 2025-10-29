import pandas as pd
import os
import datetime
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import traceback

def importar_a_mysql():
    
    ruta_base = "/opt/airflow/data/entrada"
    
    
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_VENTAS")
    tabla = "tabla_ventas_sandbox"

    

    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

    fecha_hoy = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y-%m-%d')
    
    archivos_encontrados = False

    for root, dirs, files in os.walk(ruta_base):
        for f in files:
            if f.lower().endswith('ventas.xlsx'):
                fecha_archivo = f.split('_')[0]
                if fecha_archivo == fecha_hoy:
                    archivos_encontrados = True
                    ruta_archivo = os.path.join(root, f)

                    # Leer Excel
                    df = pd.read_excel(ruta_archivo)

                    # Importar a MySQL
                    df.to_sql(tabla, con=engine, if_exists='append', index=False)
                    print(f"Archivo '{f}' importado a MySQL en tabla '{tabla}'.")

    if not archivos_encontrados:
        print(f"No se encontraron archivos con fecha de hoy ({fecha_hoy}) en {ruta_base}.")

def enviar_mail_error(**context):
    remitente = os.getenv("SMTP_MAIL_FROM")
    destinatario = os.getenv("SMTP_MAIL_FROM")
    contraseña = os.getenv("SMTP_PASSWORD")  #

    
    ti = context.get('ti')
    dag_id = context.get('dag').dag_id if context.get('dag') else 'desconocido'
    task_id = ti.task_id if ti else 'desconocido'
    timestamp = context.get('ts', 'desconocido')

    # Obtener excepción completa
    
    exc = context.get('exception')
    if exc:
        error_info = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    else:
        error_info = 'No se recibió excepción explícita.'

    # Construir mensaje
    asunto = f"Error en DAG: {dag_id}, tarea: {task_id}"
    mensaje = f"""
    Se produjo un error durante la ejecución del DAG.

    DAG: {dag_id}
    Tarea: {task_id}
    Fecha/Hora: {timestamp}
    """


    msg = MIMEMultipart()
    msg["From"] = remitente
    msg["To"] = destinatario
    msg["Subject"] = asunto
    msg.attach(MIMEText(mensaje, "plain"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(remitente, contraseña)
            server.send_message(msg)
            print("Correo de error enviado correctamente.")
    except Exception as e:
        print(f"Error al enviar correo: {e}")
        


with DAG(
    dag_id="mover_archivos_excel_sql_ventas",
    start_date=datetime.datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["excel_sql", "etl"],
) as dag:

    cargar_mysql = PythonOperator(
        task_id="cargar_archivos_mysql",
        python_callable=importar_a_mysql,
    )

    enviar_error = PythonOperator(
        task_id="enviar_mail_error",
        python_callable=enviar_mail_error,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED,  
    )

    cargar_mysql >> enviar_error 

