from bs4 import BeautifulSoup
import requests
import polars as pl
from pathlib import Path
from schemas.schemas import schema_multiple
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import time
import json
import logging
from scripts_registry import ejecutar_script, SCRIPTS_APP
from utils.logger import get_logger
from utils.utils import limpiar_terminal, guardar_json


def scrapear(logger: logging) -> date:
    """_summary_
    scrapear: Esta función scrapea la pagina web Randstad en busca de los empleos publicados el dia de hoy

    Args:
        logger (logging): Recibe el Logger de la pagina para llevar los registros con el mismo formato en el mismo archivo

    Returns:
        date: Retorna la fecha de hoy (today) si logró scrapear la pagina correctamente, de lo contrario fue que no lo consiguió
    """
    encontro_algo_de_data = False
    today = datetime.now(ZoneInfo("America/Caracas")).date()
    ayer = today - timedelta(days=1)

    plataforma = 'Randstad'
    modalidad = 'Sin data'
    experiencia = 'Sin Data'
    tipo_contrato = 'Sin Data'
    tipo_jornada = 'Sin Data'
    requisitos = 'Sin Data'

    carpeta = Path("data/historico_scraping")
    archivo = plataforma + "_" + str(today) + ".csv"
    ruta = carpeta / archivo

    encontro_condicion_finalizar = False
    page = 1
    keys = ('Fecha', 'Plataforma', 'Provincia', 'Localidad', 'Oferta_Empleo', 'Salario', 'Modalidad',
            'Tipo_Contrato', 'Tipo_Jornada', 'Experiencia', 'Empresa', 'Requisitos')

    headers = {"User-Agent": "Mozilla/5.0"}

    logger.info(f"Iniciando scraping en plataforma: {plataforma}")
    data = []
    while not encontro_condicion_finalizar:
        if page == 30:
            break
        if page == 1:
            url = "https://www.randstad.es/candidatos/ofertas-empleo/sa-1100/st-3/jp-50/"
        else:
            url = "https://www.randstad.es/candidatos/ofertas-empleo/sa-1100/st-3/jp-50/pg-" + str(page) + "/"
            time.sleep(30)

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.warning(f"No se pudo cargar la pagina {page} - URL: {url}")
            time.sleep(60)
            continue

        logger.info(f"Pagina {page} cargada correctamente - URL: {url}")
        page += 1
        soup = BeautifulSoup(response.text, "html.parser")

        # CONTENEDOR PRINCIPAL DE TODAS LAS OFERTAS
        job_container = soup.find("div", class_="rand-job-search-results")

        # ELEMENTO ul LISTA DE LAS OFERTAS
        job_list = job_container.find("ul") if job_container else None
        jobs = job_list.find_all("li") if job_list else []

        for job in jobs:
            # OFERTA DE EMPLEO
            title_tag = job.find("h3")
            try:
                oferta_empleo = title_tag.get_text(strip=True)
            except Exception as e:
                oferta_empleo = None
                logger.exception(f"Error al extraer oferta de empleo -> {e}")

            # PROVINVIA Y LOCALIDAD
            prov_loc_tag = job.find("p", class_="cards__subtitle")
            try:
                if prov_loc_tag:
                    prov_loc = prov_loc_tag.get_text(strip=True)
                    localidad = prov_loc.split("-")[0].strip()
                    provincia = prov_loc.split("-")[1].strip()
                else:
                    localidad = 'Sin Data'
                    provincia = 'Sin Data'
            except Exception as e:
                logger.exception(f"Error al extraer provincia/localidad -> {e}")
                localidad = 'Sin Data'
                provincia = 'Sin Data'

            # PIE - SALARIO Y FECHA
            footer_tag = job.find("div", class_="cards__footer")
            try:
                salary_tag = footer_tag.find("div", class_="cards__salary-info")
                if salary_tag:
                    span_salary = salary_tag.find("span")
                    salary = span_salary.get_text(strip=True)
                else:
                    salary = "Sin Data"
            except Exception as e:
                logger.exception(f"Error al extraer salario -> {e}")
                salary = 'Sin Data'

            try:
                date_tag = footer_tag.find("div", class_="cards__time-info")
                span_date = date_tag.find("span")
                date_str = span_date.get_text(strip=True).replace("desde ", "")
                date_oferta = datetime.strptime(date_str, "%d/%m/%Y").date()
            except Exception as e:
                logger.exception(f"Error al extraer fecha de oferta -> {e}")
                date_oferta = None

            # EMPRESA
            logo_tag = job.find("div", class_="cards__logo")
            try:
                if logo_tag:
                    logo_img = logo_tag.find("img")
                    empresa = logo_img.get("src")
                    empresa = empresa.split("/")[-1].replace("logo_", "").replace(".svg", "").replace("_", " ").capitalize()
                else:
                    empresa = "Sin Data"
            except Exception as e:
                logger.exception(f"Error al extraer Empresa de la oferta -> {e}")
                empresa = 'Sin Data'

            encontro_condicion_finalizar = True
            if oferta_empleo and date_oferta == today:
                encontro_algo_de_data = True
                values = (today, plataforma, provincia, localidad, oferta_empleo, salary, modalidad,
                          tipo_contrato, tipo_jornada, experiencia, empresa, requisitos)
                diccionario = schema_multiple(values=values, keys=keys)
                data.append(diccionario)
                encontro_condicion_finalizar = False

    if len(data) > 0:
        try:
            df = pl.DataFrame(data)
            df.write_csv(ruta)
            logger.info(f"Scraping finalizado. Datos guardados en: {ruta}")
            return today, encontro_algo_de_data
        except Exception as e:
            logger.error(f"Error a la hora de cargar -> {e}")
    return ayer, encontro_algo_de_data


def main(proviene_de_distribuye: bool = False) -> None:
    """_summary_
    main: punto de entrada al script dentro de la App tipo Typer.  Inicializa el Logger y controla una serie de intentos
          en caso de que no logre scrapear la pagina web correspondiente en el instante de lanzado el script
          al finalizar lanza el script de `despertar_api` para mantener la Api activa
    """
    logger = get_logger("RandstadScraper")
    carpeta = Path("data/variables")
    ruta_control_ejecusiones = carpeta / "control_ejecusiones.json"

    today = datetime.now(ZoneInfo("America/Caracas")).date()
    limpiar_terminal()
    ultimo_randstad = today - timedelta(days=1)
    encontro_algo_de_data = False

    intentos = 1
    while intentos < 5 and ultimo_randstad < today and (intentos == 1 or encontro_algo_de_data):
        logger.info(f"***********  Intento {intentos} de scrapear Randstad  ********************")
        try:
            with ruta_control_ejecusiones.open("r", encoding="utf-8") as f:
                control_ejecusiones = json.load(f)
            logger.info("El json control_ejecusiones cargo correctamente")
            intentos += 1
        except Exception as e:
            logger.error(f"Error al intentar cargar control_ejecusiones en el intento {intentos} -> {e}")
            time.sleep(150)
            intentos += 1
            continue

        ultimo_randstad = date.fromisoformat(control_ejecusiones["ultima_ejecusion_randstad_scraping"])
        logger.info(f"Ultima vez que se scrapeo Randstad fue {ultimo_randstad}")

        ultimo_randstad, encontro_algo_de_data = scrapear(logger=logger)
        if ultimo_randstad < today and encontro_algo_de_data:
            time.sleep(150)
        else:
            try:
                if encontro_algo_de_data:
                    control_ejecusiones["ultima_ejecusion_trabajoscom_scraping"] = str(today)
                    guardar_json(archivo=control_ejecusiones, ruta=ruta_control_ejecusiones)
                    logger.info(f"Se actualizo la fecha de la ultima vez scraper a --> {today}")
                    logger.info("Se ha actualizado el archivo -->  control_ejecusiones.json")
                else:
                    logger.info("NO ACTUALIZO --> control_ejecusiones.json,  porque NO ENCONTRO DATA")
            except Exception as e:
                logger.error(f"Error actualizacon archivo control_ejecusiones.json -> {e}")

        if intentos == 5:
            logger.info("DESPUES DE 4 INTENTOS NO LOGRO SCRAPEAR RANDSTAD")

    if proviene_de_distribuye:
        ejecutar_script(SCRIPTS_APP["tecnoempleo"], proviene_de_distribuye=True)
    return
