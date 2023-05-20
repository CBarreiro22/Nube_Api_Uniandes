import base64
import hashlib
import io
import tempfile
import uuid

from google.cloud import storage
from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import exceptions

from flask import send_file, make_response
from flask_restful import Resource
from flask_jwt_extended import jwt_required, get_jwt_identity, create_access_token
from flask import request
from modelos import db, Usuario, Tarea, TareaSchema
import os
import logging

# Configuración del registro para la consola
logging.basicConfig(format='%(levelname)s:%(asctime)s:%(message)s', level=logging.DEBUG)

from werkzeug.utils import secure_filename

ALLOWED_EXTENSIONS = {'ZIP', '7Z', 'TAR.GZ', 'TAR.BZ2'}
ROOT_PATH = '/nfs/apis_nube'

tarea_schema = TareaSchema()

queue = pubsub_v1.PublisherClient()
topic_path = queue.topic_path('api-nube-semana-3', 'my-topic')
project_id = 'api-nube-semana-3'
topic_id = 'my-topic'
max_retries = 3


class VistaSignup(Resource):
    def post(self):

        username = request.json["username"]
        password1 = request.json["password1"]
        password2 = request.json["password2"]
        email = request.json["email"]
        if password1 != password2:
            return {"mensaje": "la cuenta no pudo ser creada, passwords proporcionados no coinciden."}, 404
        if len(password1) < 8:
            return {"mensaje": "la cuenta no pudo ser creada, longitud de password debe ser mayor a 8 caracteres."}, 404
        usuario = Usuario.query.filter(Usuario.username == username).first()
        if usuario is not None:
            return {"mensaje": "la cuenta no pudo ser creada, username ya existe."}, 404
        usuario = Usuario.query.filter(Usuario.email == email).first()
        if usuario is not None:
            return {"mensaje": "la cuenta no pudo ser creada, email ya existe."}, 404
        password_encriptado = hashlib.md5(
            request.json["password1"].encode('utf-8')).hexdigest()
        nuevo_usuario = Usuario(
            username=username, password=password_encriptado, email=email)
        db.session.add(nuevo_usuario)
        db.session.commit()
        return {"mensaje": "cuenta creada con éxito"}, 200


class VistaLogin(Resource):
    def post(self):
        username = request.json.get("username", None)
        email = request.json.get("email", None)
        password_encriptado = hashlib.md5(
            request.json["password"].encode('utf-8')).hexdigest()

        if email is not None:
            usuario = Usuario.query.filter(
                Usuario.email == email, Usuario.password == password_encriptado).first()
            db.session.commit()
            if usuario is None:
                return {"mensaje": "cuenta no existe"}, 404
        elif username is not None:
            usuario = Usuario.query.filter(
                Usuario.username == username, Usuario.password == password_encriptado).first()
            db.session.commit()
            if usuario is None:
                return {"mensaje": "cuenta no existe"}, 404
        else:
            # Handle the case where email is None
            return {"mensaje": "correo electrónico no proporcionado"}, 400

        token_acceso = create_access_token(identity=usuario.username)
        return {"token": token_acceso}, 200


class VistaTask(Resource):
    @jwt_required()
    def get(self, id_task):
        return tarea_schema.dump(Tarea.query.with_entities(Tarea.id, Tarea.file_name, Tarea.file_name_converted,
                                                           Tarea.time_stamp, Tarea.new_format, Tarea.status).filter(
            Tarea.id == id_task).first())

    @jwt_required()
    def delete(self, id_task):
        tarea = Tarea.query.get_or_404(id_task)
        db.session.delete(tarea)
        db.session.commit()
        return '', 204


class VistaTasks(Resource):
    @jwt_required()
    def get(self):
        args = request.args
        query_max = args.get('max') or None
        query_order = args.get('order') or None
        if query_max != None and not query_max.isnumeric():
            return {"mensaje": "max debe ser numerico"}, 400
        if query_order != None and query_order not in ('0', '1'):
            return {"mensaje": "order debe ser numerico: 0 o 1"}, 400
        if (query_order == '1'):
            tareas = Tarea.query.with_entities(Tarea.id, Tarea.file_name, Tarea.file_name_converted,
                                               Tarea.time_stamp, Tarea.new_format, Tarea.status).order_by(
                Tarea.id.desc()).limit(query_max)
        else:
            tareas = Tarea.query.with_entities(Tarea.id, Tarea.file_name, Tarea.file_name_converted,
                                               Tarea.time_stamp, Tarea.new_format, Tarea.status).limit(query_max)
        return [tarea_schema.dump(tarea) for tarea in tareas]

    import tempfile

    @jwt_required()
    def post(self):
        archivo = request.files['file']
        new_format = request.form["newFormat"]
        if archivo.filename == '':
            return {"mensaje": "file no proporcionado"}
        if not allowed_file(archivo.filename):
            return {"mensaje": "file no soportado"}

        if archivo:
            filename = secure_filename(archivo.filename)
            file_name_converted = os.path.splitext(filename)[0] + '.' + new_format
            current_user = Usuario.query.filter(Usuario.username == get_jwt_identity()).first()
            nueva_tarea = Tarea(
                file_name=filename.lower(),
                file_name_converted=file_name_converted.lower(),
                new_format=new_format,
                usuario=current_user.id,
                file_path='',
                file_path_converted=''
            )
            db.session.add(nueva_tarea)
            db.session.commit()

            with tempfile.TemporaryDirectory() as temp_dir:
                file_path = os.path.join(temp_dir, filename)
                file_path_converted = os.path.join(temp_dir, file_name_converted)
                archivo.save(file_path)
                nueva_tarea.file_path = file_path

                bytes_io = io.BytesIO()
                archivo.save(bytes_io)
                
                id_file = 'archivo' + str(uuid.uuid4())
                #id_file = f"{nueva_tarea.id}/{nueva_tarea.file_name}"
                url = upload_file_to_gcs('pruebaapisnube', file_path, id_file)
                if url:
                    nueva_tarea.file_path_converted = url
                    logging.debug('url del archivo: %s', str(url))
                    # Eliminar el archivo de la carpeta temporal
                    os.remove(file_path)
                    # Enviar mensaje a pub/sub
                    message = id_file + "," + new_format + "," + str(nueva_tarea.id)
                    self.publish_message(message)
                db.session.commit()
        return {"mensaje": "procesado con éxito"}

    def publish_message(self, message):
        # Crea un cliente de Pub/Sub
        publisher = pubsub_v1.PublisherClient()

        # Forma el nombre completo del tema
        topic_path = publisher.topic_path(project_id, topic_id)

        # Convierte el mensaje en bytes
        data = message.encode('utf-8')

        # Publica el mensaje en el tema
        future = publisher.publish(topic_path, data)

        # Espera a que se complete la publicación del mensaje
        future.result()


class VistaFiles(Resource):
    @jwt_required()
    def get(self, filename):
        filename = secure_filename(filename)
        task = Tarea.query.filter(Tarea.file_name == filename.lower()).order_by(
            Tarea.time_stamp.desc()).first()
        is_original = True
        if task is None:
            task = Tarea.query.filter(Tarea.file_name_converted == filename.lower()).order_by(
                Tarea.time_stamp.desc()).first()
            if task is None:
                return {"mensaje": "filename no existe"}, 404
            else:
                is_original = False
        response = download_file_converted(task, filename, is_original)
        return response


class VistaFile(Resource):
    @jwt_required()
    def get(self, id_task):
        task = Tarea.query.get_or_404(id_task)
        return {"url": task.file_path_converted}


def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].upper() in ALLOWED_EXTENSIONS


def download_file_converted(task, file_name, is_original):
    if is_original:
        # create a file-like object from the bytes
        tar_file = io.BytesIO(task.file_data_name)
    else:
        tar_file = io.BytesIO(task.file_data_converted)
    # create a response object
    response = make_response(tar_file.getvalue())
    # set the Content-Disposition header to trigger a file download
    response.headers.set('Content-Disposition',
                         'attachment', filename=file_name)
    # set the MIME type for the response
    response.headers.set('Content-Type', 'application/x-gzip')
    # return the response
    return response


def upload_file_to_gcs(bucket_name, source_blob, destination_blob_name):
    """
    Sube un archivo a un bucket de Google Cloud Storage

    Args:
        bucket_name (str): Nombre del bucket
        source_file_path (str): Ruta del archivo local a subir
        destination_blob_name (str): Nombre del archivo en el bucket
        max_retries (int): Número máximo de intentos de carga (por defecto: 3)

    Returns:
        str: URL del archivo subido

    Raises:
        Exception: Si se produce un error al cargar el archivo en Google Cloud Storage
    """

    # Crea una instancia del cliente de Google Cloud Storage
    storage_client = storage.Client()

    # Obtiene el bucket
    bucket = storage_client.bucket(bucket_name)

    # Crea un objeto Blob en el bucket
    blob = bucket.blob(destination_blob_name)

    blob._properties["timeout"] = 600

    # Intenta cargar el archivo con varios intentos
    for attempt in range(max_retries):
        try:
            # Carga el archivo en el objeto Blob
            blob.upload_from_filename(source_blob)

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
