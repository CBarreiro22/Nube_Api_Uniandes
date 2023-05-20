from flask import Flask
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import exceptions


from .modelos import db, Tarea
import io
import shutil
import tarfile
import zipfile
import py7zr
from google.cloud import pubsub_v1
import logging
from google.cloud import storage
import os

bucket_name = 'pruebaapisnube'

# Configuración del registro para la consola
logging.basicConfig(format='%(levelname)s:%(asctime)s:%(message)s', level=logging.DEBUG)

IP = '10.128.0.7'

max_retries = 3

def create_app(config_name):
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://admin:admin@{IP}:5432/apisnube'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['PROPAGATE_EXCEPTIONS'] = True
    db.init_app(app)
    return app


app = create_app('default')
app_context = app.app_context()
app_context.push()
db.create_all()

project_id = 'api-nube-semana-3'
topic_name = 'my-topic'
subscriber_name = 'my-subscriber'
topic_path = f"projects/{project_id}/topics/{topic_name}"

import tempfile


def download_file_from_gcs(blob_name):
    """
    Descarga un archivo desde un bucket de Google Cloud Storage utilizando el nombre del archivo en el bucket.

    Args:
        bucket_name (str): Nombre del bucket
        blob_name (str): Nombre del archivo en el bucket

    Returns:
        str: Ruta del archivo descargado
    """

    # Crea una instancia del cliente de Google Cloud Storage
    storage_client = storage.Client()

    # Obtiene el bucket
    bucket = storage_client.bucket(bucket_name)

    # Obtiene el objeto Blob
    blob = bucket.blob(blob_name)

    # Crea un archivo temporal para guardar la descarga
    temp_dir = tempfile.gettempdir()
    logging.debug("TEMPORALLLLL ****************************"+temp_dir)
    destination_file_path = os.path.join(temp_dir, blob_name)

    # Descarga el archivo en el objeto Blob al archivo local
    blob.download_to_filename(destination_file_path)

    return destination_file_path


def convert_file_tar_gz(id_task, file):
    tarea = Tarea.query.get_or_404(id_task)
    # extract the contents of the ZIP file to a temporary directory
    with zipfile.ZipFile(file, 'r') as zip_ref:
        tmp_dir = 'tmp'
        zip_ref.extractall(tmp_dir)

    # create the TAR.GZ file from the temporary directory
    with io.BytesIO() as tar_buffer:
        with tarfile.open(fileobj=tar_buffer, mode='w:gz') as tar_ref:
            tar_ref.add(tmp_dir, arcname='.')

        # get the bytes of the TAR.GZ file
        tar_bytes = tar_buffer.getvalue()
    with open(tarea.file_path_converted, 'wb') as archivo:
        archivo.write(tar_bytes)
    # tarea.file_data_converted = tar_bytes
    # db.session.commit()

    # delete the temporary directory
    shutil.rmtree(tmp_dir)


def convert_file_tar_bz2(id_task, file):
    tarea = Tarea.query.get_or_404(id_task)

    # extract the contents of the ZIP file to a temporary directory
    with zipfile.ZipFile(file, 'r') as zip_ref:
        tmp_dir = 'tmp'
        zip_ref.extractall(tmp_dir)

    # create the TAR.BZ2 file from the temporary directory
    with io.BytesIO() as tar_buffer:
        with tarfile.open(fileobj=tar_buffer, mode='w:bz2') as tar_ref:
            tar_ref.add(tmp_dir, arcname='.')

        # get the bytes of the TAR.BZ2 file
        tar_bytes = tar_buffer.getvalue()

    with open(tarea.file_path_converted, 'wb') as archivo:
        archivo.write(tar_bytes)
    # tarea.file_data_converted = tar_bytes
    # db.session.commit()

    # delete the temporary directory
    shutil.rmtree(tmp_dir)


def convert_file_7z(id_task, file):
    tarea = Tarea.query.get_or_404(id_task)
    # extract the contents of the ZIP file to a temporary directory
    with zipfile.ZipFile(file, 'r') as zip_ref:
        tmp_dir = 'tmp'
        zip_ref.extractall(tmp_dir)

    # create the 7Z file from the temporary directory
    with io.BytesIO() as archive_buffer:
        with py7zr.SevenZipFile(archive_buffer, 'w') as archive_ref:
            archive_ref.writeall(tmp_dir)

        # get the bytes of the 7Z file
        archive_bytes = archive_buffer.getvalue()
    with open(file+".7z", 'wb') as archivo:
        archivo.write(archive_bytes)
    # tarea.file_data_converted = archive_bytes
    # db.session.commit()
    upload_file_to_gcs(bucket_name, file, file.lstrip("/tmp/")+".7z")
    # delete the temporary directory
    shutil.rmtree(tmp_dir)



def get_file_by_id_task(id_task):
    tarea = Tarea.query.get_or_404(id_task)

    with open(tarea.file_path, 'rb') as file:
        return io.BytesIO(file.read())


def process_to_convert(new_format, file_name,nueva_tarea_id):
    logging.debug("process_to_convert !!!!!!!!!!!!!!!!!!!!!!!!!!!!", new_format, file_name)
    file = download_file_from_gcs(file_name)
    logging.debug("file: %s", str(file))
    if new_format.upper() == 'TAR.GZ':
        convert_file_tar_gz(nueva_tarea_id, file)
    elif new_format.upper() == '7Z':
        convert_file_7z(nueva_tarea_id, file)
    elif new_format.upper() == 'TAR.BZ2':
        convert_file_tar_bz2(nueva_tarea_id, file)



def callback(message):
    with app.app_context():
        print("calbackkkkk")
        message.ack()
        print(f"Mensaje recibido: {message.data.decode()}")
        print("Legoooooooooooooooooooo " + str(message.data.decode()))
        print(message.data)
        # Realiza cualquier procesamiento adicional que desees hacer con el mensaje aquí
        data = str(message.data.decode()).split(",")
        file_name = data[0]
        format_to_convert = data[1]
        file_id = data[2]
        logging.debug(file_name + "   " + format_to_convert + "   " + file_id)
        process_to_convert(file_name=file_name, new_format=format_to_convert, nueva_tarea_id=file_id)



from google.cloud import storage

def upload_file_to_gcs(bucket_name, local_file_path, destination_blob_name):
    """Sube un archivo local a un bucket de GCS."""
    # Crea una instancia del cliente de almacenamiento de GCS
    client = storage.Client()
    # Obtén una referencia al bucket
    bucket = client.bucket(bucket_name)
    # Crea un nuevo blob en el bucket
    blob = bucket.blob(destination_blob_name)
    # Carga el archivo local al blob en GCS
    blob.upload_from_filename(local_file_path)
    for attempt in range(max_retries):
        try:
            # Carga el archivo en el objeto Blob
            blob.upload_from_filename(local_file_path)

            # Obtiene la URL pública del archivo subido
            url = blob.public_url

            return url
        except (GoogleAPICallError, exceptions.GoogleCloudError, exceptions.RetryError) as e:
            if attempt < max_retries - 1:
                # En caso de error, se hace un nuevo intento
                print(
                    f"Error al cargar el archivo en Google Cloud Storage. Intento {attempt + 1}/{max_retries}. Error: {e}")
            else:
                logging.error(
                    "Error al cargar el archivo en Google Cloud Storage. Se excedió el número máximo de intentos.")
                # Si todos los intentos fallan, se lanza una excepción
                return




def subscribe():
    # Crea un cliente de Pub/Sub
    subscriber = pubsub_v1.SubscriberClient()

    # Crea el nombre completo de la suscripción
    subscription_path = f"projects/{project_id}/subscriptions/{subscriber_name}"

    # Inicia la suscripción y especifica la función de devolución de llamada
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    # Espera a que la suscripción se mantenga activa
    try:
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        raise


subscribe()
