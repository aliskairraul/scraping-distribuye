import requests
from pathlib import Path
from datetime import datetime, date
import json
import sys
import time
from utils.utils import limpiar_terminal, guardar_json
from utils.logger import get_logger
# from scripts_registry import ejecutar_script, SCRIPTS_APP

carpeta = Path("data/db")
ruta = carpeta / "base.parquet"


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
    except TimeoutError:
        return 500


def main():
    """_summary_
    main: Entrada a este script de la app.
          Lleva el control de los logs y mantiene actualizado el registro de las ultimas ejecusión del envio a la Api
    """
    # DESPERTAR LA API ANTES DE ENVIAR LA DATA
    # ejecutar_script(SCRIPTS_APP["despertar_api"], maximo_intentos=2, limpiar=False)

    today = datetime.now().date()
    carpeta = Path("data/variables")
    ruta_control_ejecusiones = carpeta / "control_ejecusiones.json"
    logger = get_logger("Envia-Api")
    logger.info("********  Inicia Envio de Data Actualizada a API **************************")

    # limpiar_terminal()
    status_code = 0

    intentos = 1
    while intentos < 5 and status_code != 200:
        try:
            with ruta_control_ejecusiones.open("r", encoding="utf-8") as f:
                control_ejecusiones = json.load(f)
            logger.info("El json control_ejecusiones cargo correctamente")
            intentos += 1
        except TimeoutError:
            logger.error(f"Error al intentar cargar control_ejecusiones en el intento {intentos}")
            time.sleep(450)
            intentos += 1
            continue
        status_code = enviar_data_api()
        if status_code != 200:
            logger.error(f"No se pudo enviar la Data a la Api en el intento -->  {intentos - 1}")
            time.sleep(450)
            continue

        logger.info(f"Se logro enviar la data a la Api en el intento --> {intentos - 1}")
        control_ejecusiones["ultima_ejecusion_enviar_api"] = str(today)
        try:
            guardar_json(archivo=control_ejecusiones, ruta=ruta_control_ejecusiones)
            logger.info("Se Actualizo la fecha en el control de Ejecusiones")
        except TimeoutError:
            logger.error("No se pudo actualizar la fecha en el control de ejecusiones a pesar de que si envio a la Api la data")

    sys.exit()
