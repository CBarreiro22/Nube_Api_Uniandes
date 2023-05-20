# Usamos una imagen base de Python
FROM python:3.9
# Establecemos el directorio de trabajo en el contenedor
WORKDIR /app
# Copiamos los archivos de requerimientos al contenedor
COPY requirements.txt .
# Instalamos las dependencias del proyecto
RUN pip install --no-cache-dir -r requirements.txt
# Copiamos los archivos de la aplicación al contenedor
COPY . .
# Exponemos el puerto en el que se ejecutará la aplicación
EXPOSE 5000
# Definimos el comando por defecto que se ejecutará cuando el contenedor inicie
CMD ["python", "app.py"]