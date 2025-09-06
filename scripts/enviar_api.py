import requests
from pathlib import Path
from datetime import datetime, date
import json
import os
import time
from utils.utils import limpiar_terminal, guardar_json
import logging
from utils.logger import get_logger
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Alcances (Scopes) necesarios para interactuar con Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive']

carpeta = Path("data/db")
ruta = carpeta / "base.parquet"


def subir_a_google_drive(ruta_archivo: Path, nombre_archivo_gd: str, logger: logging) -> bool:
    """
    Sube un archivo a Google Drive usando las credenciales de la cuenta de servicio.
    """
    try:
        # Carga las credenciales desde el secreto de GitHub
        creds_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
        if not creds_json:
            raise ValueError("La variable de entorno GOOGLE_DRIVE_CREDENTIALS no está configurada.")

        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

        service = build('drive', 'v3', credentials=creds)

        file_metadata = {'name': nombre_archivo_gd}
        media = MediaFileUpload(ruta_archivo, resumable=True)

        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        logger.info(f"Archivo subido, ID: {file.get('id')}")
        return True
    except Exception as e:
        logger.error(f"Ocurrió un error al subir el archivo a Google Drive: {e}")
        return False


def enviar_data_api() -> int:
    """_summary_
    enviar_data_api: envia el archivo parquet con la data actualizada a la Api

    Returns:
        int: Retorna el status code de la operacion para saber si cumplió su cometido
    """
    url = "https://primeraapirender.onrender.com/upload-parquet/"
    try:
        with open(str(ruta), "rb") as f:
            files = {"file": ("db_local.parquet", f, "application/octet-stream")}
            response = requests.post(url, files=files)
            return response.status_code
    except Exception:
        return 500


def main():
    """_summary_
    main: Entrada a este script de la app.
          Lleva el control de los logs y mantiene actualizado el registro de las ultimas ejecusión del envio a la Api
    """
    today = datetime.now().date()
    carpeta = Path("data/variables")
    ruta_control_ejecusiones = carpeta / "control_ejecusiones.json"
    logger = get_logger("Envia-Api")
    logger.info("********  Inicia Envio de Data Actualizada a API **************************")

    # limpiar_terminal()
    status_code = 0
    intentos_api = 1
    intentos_gd = 1
    intentos_ce = 1
    logro_cargar_control_ejecusiones = False

    #  Bucle para cargar la variable de control
    while intentos_ce < 5 and not logro_cargar_control_ejecusiones:
        intentos_ce += 1
        try:
            with ruta_control_ejecusiones.open("r", encoding="utf-8") as f:
                control_ejecusiones = json.load(f)
            logger.info("El json control_ejecusiones cargo correctamente")
            logro_cargar_control_ejecusiones = True
            break
        except Exception:
            logger.error(f"Error al intentar cargar control_ejecusiones en el intento {intentos_ce - 1}")
            time.sleep(300)

    # Bucle para envia Data a la Api
    while intentos_api < 5 and status_code != 200:
        intentos_api += 1
        try:
            status_code = enviar_data_api()
            if status_code == 200:
                logger.info("Archivo 'base.parquet' subido exitosamente a la Api.")
                if logro_cargar_control_ejecusiones:
                    control_ejecusiones["ultima_ejecusion_enviar_api"] = str(today)
                logger.info(f"Se logro enviar la data a la Api en el intento --> {intentos_api - 1}")
                break
            else:
                logger.error(f"No se pudo enviar la Data a la Api en el intento -->  {intentos_api - 1}")
                time.sleep(300)
        except Exception as e:
            logger.error(f"Error inesperado al subir a la Api: {e}")
            time.sleep(300)

    # Bucle de reintentos para Google Drive
    logro_grabar_google_drive = False
    while intentos_gd < 5 and not logro_grabar_google_drive:
        intentos_gd += 1
        try:
            logro_grabar_google_drive = subir_a_google_drive(ruta_archivo=ruta, nombre_archivo_gd="base.parquet", logger=logger)
            if logro_grabar_google_drive:
                logger.info("Archivo 'base.parquet' subido exitosamente a Google Drive.")
                if logro_cargar_control_ejecusiones:
                    control_ejecusiones["ultima_ejecusion_enviar_gd"] = str(today)
                break  # Salir del bucle si es exitoso
            else:
                logger.error(f"No se pudo subir a Google Drive en el intento --> {intentos_gd}")
                time.sleep(150)  # Esperar antes de reintentar
        except Exception as e:
            logger.error(f"Error inesperado al subir a Google Drive: {e}")
            time.sleep(150)

    if logro_cargar_control_ejecusiones:
        try:
            guardar_json(archivo=control_ejecusiones, ruta=ruta_control_ejecusiones)
            if status_code == 200:
                logger.info("Se Actualizo la fecha en el control de Ejecusiones para el envio de la Api")
            if logro_grabar_google_drive:
                logger.info("Se Actualizo la fecha en el control de Ejecusiones para el envio a Google Drive")
        except Exception:
            logger.error("No se pudo actualizar la fecha en el control de ejecusiones")

    return
