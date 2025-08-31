import requests
from pathlib import Path
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import json
import sys
import time
import os
from utils.utils import limpiar_terminal, guardar_json, borrar_archivos
from utils.logger import get_logger


def main(maximo_intentos: int = 4, limpiar: bool = True) -> None:
    """_summary_
    main: Funcion de entrada al script `despertar_api` y en este caso ejecuta la logica de hacer un llamado sencillo varias veces
          a la Api para mantenerla activa y que este disponible a la brevedad cuando sea solicitada por la App del dashboard, Esta
          función se ejecuta Todas las Horas del dia.

    Args:
        maximo_intentos (int, optional): _description_. Defaults to 5. En caso de que corresponda a una hora donde se realice un script de scrapeo
                                                                       el numero de intentos será menor
        limpiar (bool, optional): _description_. Defaults to True.     En caso de que sea llamada desde la ejecusión de un script de scrapeo y no desde
                                                                       el launcher principal no limpiara la terminal para seguir con la secuencia de la
                                                                       acción
    """
    today = datetime.now().date()
    hora_local = datetime.now(ZoneInfo("America/Caracas")).hour

    carpeta = Path("data/variables")
    ruta_control_ejecusiones = carpeta / "control_ejecusiones.json"
    logger = get_logger("Despertar-api")

    if limpiar:
        limpiar_terminal()
    else:
        logger.info("********* LLAMADA desde algun SCRAPER *************")
    for intentos in range(1, maximo_intentos, 1):
        logger.info(f"Inicio del ciclo {intentos} de la hora {hora_local}")
        try:
            with ruta_control_ejecusiones.open("r", encoding="utf-8") as f:
                control_ejecusiones = json.load(f)
            logger.info("El json control_ejecusiones cargo correctamente")
        except TimeoutError:
            logger.error(f"Error al intentar cargar control_ejecusiones en el intento {intentos}")

        response = requests.get("https://primeraapirender.onrender.com/wake-up/")

        if response.status_code == 200:
            logger.info("EL status code fue 200 despertando a la API")
            control_ejecusiones["ultima_hora_despertar_api"] = hora_local
            control_ejecusiones["ultimo_dia_despertar_api"] = str(today)
        else:
            logger.error(f"El status code FUE {response.status_code} intentando despertar a la Api")

        if hora_local == 2:
            archivos_borrar = []
            fecha_obsolecencia = today - timedelta(days=15)
            logs: list = os.listdir("logs")
            for archivo in logs:
                fecha_str = archivo.split("_")[2].replace(".log", "").strip()
                try:
                    if datetime.strptime(fecha_str, "%Y-%m-%d").date() <= fecha_obsolecencia:
                        archivos_borrar.append(archivo)
                except ValueError:
                    logger.error("ERROR 500 el archivo tipo log tiene un nombre no previsto")

            if len(archivos_borrar) > 0:
                borrar_archivos(archivos=archivos_borrar, carpeta="logs", logger=logger)

        time.sleep(600)

    try:
        guardar_json(archivo=control_ejecusiones, ruta=ruta_control_ejecusiones)
        logger.info("Se actualizo el archivo de control de ejecusiones")
    except TimeoutError:
        logger.error("Error tratando de Actualizar el archivo de control de ejecusiones")

    sys.exit()
