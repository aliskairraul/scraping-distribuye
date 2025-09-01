import polars as pl
from pathlib import Path
from datetime import datetime, timedelta, date
import time
import os
import sys
import logging
import json
from utils.logger import get_logger
from utils.utils import limpiar_terminal, guardar_json
# from scripts_registry import ejecutar_script, SCRIPTS_APP


def procesar_data(logger: logging) -> date:
    """_summary_
    procesar_data: Simplemente Actualiza la data de los Empleos disponibles, juntando los empleos de hoy a los que ya se
                   encuentran y eliminando los repetidos.  Se considera oferta de empleo repetida, cuando todos los campos
                   a excepción de la `fecha de publicación` son exactos, y se queda con el que tenga la fecha mas reciente.
                   También borra aquellos registros cuya fecha ya sea de 21 dias o mas

    Args:
        logger (logging): Logger de esta parte de la aplicación para mantener en un mismo archivo los registros de lo que va
                          pasando con el codigo

    Returns:
        date: Si retorna la fecha de hoy `today` fue que logró cumplir su cometido sin ninguna novedad, de lo contrario fue que
              ocurrió algún imprevisto que rompio el codigo y en unos minutos se volverá a intentar
    """
    logger.info("Iniciando el Procesamiento de la Data")

    today = datetime.now().date()
    ayer = today - timedelta(days=1)

    fecha_obsolecencia = today - timedelta(days=21)
    carpeta_scraping = Path("data/historico_scraping")
    carpeta_db = Path("data/db")
    ruta_base = carpeta_db / "base.parquet"

    try:
        df_base = pl.read_parquet(ruta_base)
        logger.info(f"Se logró cargar la DATABASE, actualmente con {df_base.shape[0]} registros")
    except TimeoutError:
        logger.exception("Error al intentar cargar la data Base")
        return

    # Clave Unica --> Todas las columnas - 'Fecha'  (Para evitar repetidos)
    columns = df_base.columns
    columns.remove("Fecha")
    clave_unica = columns

    # En archivos_hoy  estaran los nombres de los archivos scrapeados hoy
    historico_scrapeados = os.listdir(carpeta_scraping)
    archivos_hoy = []
    archivos_borrar = []
    for archivo in historico_scrapeados:
        fecha_str = archivo.split("_")[1].replace(".csv", "").strip()
        if datetime.strptime(fecha_str, "%Y-%m-%d").date() == today:
            archivos_hoy.append(archivo)
        if datetime.strptime(fecha_str, "%Y-%m-%d").date() <= fecha_obsolecencia:
            archivos_borrar.append(archivo)

    if len(archivos_hoy) == 0:
        logger.error("No hay archivos de data scrapeada correspondiente a la fecha {today}")
        return ayer

    # Juntando TODOS los csv de Hoy en un solo Dataframe
    df_actualizacion = pl.DataFrame()
    for i, archivo in enumerate(archivos_hoy):
        ruta = carpeta_scraping / archivo
        try:
            df = pl.read_csv(ruta)
        except TimeoutError:
            logger.exception(f"No logró cargar la data que deberia esta en {ruta}")
            continue
        if i == 0:
            df_actualizacion = df
        else:
            df_actualizacion = pl.concat([df_actualizacion, df], how="vertical")

    if df_actualizacion.shape[0] == 0:
        logger.error("No hay ninguna Data disponible para actualizar")
        return ayer

    logger.info(f"Se logró cargar la data para actualizar, actualmente con {df_actualizacion.shape[0]} registros")
    df_actualizacion = df_actualizacion.with_columns(pl.col("Fecha").cast(pl.Date))

    # Juntando la Base con el Dataframe de los csv de hoy
    df_base = pl.concat([df_base, df_actualizacion], how="vertical")

    # Agrupamos por las columnas clave y nos quedamos con la fila de fecha más reciente en caso de haber alguna oferta de trabajo repetida
    df_base = (
        df_base
        .sort("Fecha", descending=True)
        .unique(subset=clave_unica, keep="first")
    )

    # Borramos los registros con Obsolecencia
    mask = df_base['Fecha'] >= fecha_obsolecencia
    df_base = df_base.filter(mask)

    try:
        df_base.write_parquet(ruta_base)
        logger.info(f"ETL finalizado, se guardaron los datos en {ruta_base}")
        logger.info(f"Despues de Juntar y eliminar ofertas repetidas quedaron {df_base.shape[0]} registros")
        return today
    except TimeoutError:
        logger.error(f"No logró guardar la actualizacion correspondiente a {today}")
    return ayer


def main() -> None:
    """_summary_
    main: Función de entrada al script que junta la data de los empleos en la Data del sistema con los empleos conseguidos el dia de hoy
          Esta función también inicializa el Logger de esta parte de la App y se encarga de realizar varios intentos en caso de que no consiga
          su objetivo a la primera.
          Después de realizar el proceso de procesamiento de la Data llama al script de `despertar_api` para mantener la Api activa para que esté
          lista para las solicitudes de la App del dashboard
    """
    logger = get_logger("ActualizacionData")
    carpeta = Path("data/variables")
    ruta_control_ejecusiones = carpeta / "control_ejecusiones.json"

    today = datetime.now().date()
    # limpiar_terminal()

    intentos = 1
    logger.info(f"**********  Intento {intentos} de añadir a la DATABASE y la data del scrapeo del dia {today}  ********")
    try:
        with ruta_control_ejecusiones.open("r", encoding="utf-8") as f:
            control_ejecusiones = json.load(f)
        logger.info("El json control_ejecusiones cargo correctamente")
    except TimeoutError:
        logger.error(f"Error al intentar cargar control_ejecusiones en el intento {intentos}")
    ultimo_etl = date.fromisoformat(control_ejecusiones["ultima_ejecusion_etl"])

    logger.info(f"Ultima vez que se scrapeo Randstad fue {ultimo_etl}")
    while intentos < 5 and ultimo_etl < today:
        ultimo_etl = procesar_data(logger=logger)
        if ultimo_etl < today:
            time.sleep(450)
            intentos += 1
        else:
            control_ejecusiones["ultima_ejecusion_etl"] = str(today)
            try:
                guardar_json(archivo=control_ejecusiones, ruta=ruta_control_ejecusiones)
                logger.info(f"Se actualizo la fecha de la ultima vez ETL a --> {today}")
                logger.info("Se ha actualizado el archivo -->  control_ejecusiones.json")
            except TimeoutError:
                logger.error("Error actualizacon archivo -->  control_ejecusiones.json")

        if intentos == 5:
            logger.info("DESPUES DE 4 INTENTOS NO LOGRO REALIZAR EL ETL")

    # ejecutar_script(SCRIPTS_APP["despertar_api"], maximo_intentos=3, limpiar=False)
    sys.exit()
