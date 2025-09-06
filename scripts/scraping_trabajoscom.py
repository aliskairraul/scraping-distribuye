from bs4 import BeautifulSoup
import requests
import polars as pl
from pathlib import Path
from schemas.schemas import schema_multiple
from datetime import datetime, timedelta, date
import time
import logging
import json
from scripts_registry import ejecutar_script, SCRIPTS_APP
from utils.logger import get_logger
from utils.utils import limpiar_terminal, guardar_json


def separa_localidad_provincia(cadena: str) -> tuple[str, str]:
    tiene_todo = False
    if "Todo" in cadena:
        tiene_todo = True
        provincia = cadena.split(" ")[-1].strip()
        localidad = "Todo " + provincia

    if not tiene_todo:
        if "," in cadena:
            localidad = cadena.split(",")[0].strip()
            provincia = cadena.split(",")[1].strip()
        else:
            localidad = cadena
            provincia = cadena

    return (localidad, provincia)


def scrapear(logger: logging) -> date:
    """_summary_
    scrapear: Esta función scrapea la pagina web Trabajos.com en busca de los empleos publicados el dia de hoy

    Args:
        logger (logging): Recibe el Logger de la pagina para llevar los registros con el mismo formato en el mismo archivo

    Returns:
        date: Retorna la fecha de hoy (today) si logró scrapear la pagina correctamente, de lo contrario fue que no lo consiguió
    """
    today = datetime.now().date()
    ayer = today - timedelta(days=1)
    plataforma = 'Trabajoscom'
    experiencia = 'Sin Data'
    requisitos = 'Sin Data'
    modalidad = 'Sin Data'

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
        if page == 1:
            url = "https://www.trabajos.com/ofertas-empleo/"
        else:
            desde = ((page - 1) * 40) + 1
            url_1 = "https://www.trabajos.com/ofertas-empleo/?CADENA=&IDPAIS=100&=NO&ORD=F&FPV=&MT-FPV=NO&FPB=&MT-FPB=NO&FAR=&MT-FAR=NO&FPF=&MT-FPF=NO&FEM=&MT-FEM=NO&FXM-S-MIN=-1&FXM-S-MAX=-1&FSM-S-MIN=-1&FSM-S-MAX=-1&FSM-TP=A&FJL=&MT-FJL=NO&FTC=&MT-FTC=NO&FBL=&MT-FBL=NO&DESDE="
            url = url_1 + str(desde) + "#google_vignette"
            time.sleep(30)

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.warning(f"No se pudo cargar la pagina {page} - URL: {url}")
            time.sleep(60)
            continue

        logger.info(f"Pagina {page} cargada correctamente - URL: {url}")
        page += 1
        soup = BeautifulSoup(response.text, "html.parser")

        job_list = soup.find_all("div", class_="listado2014 card oferta")
        for job in job_list:
            # OFERTA EMPLEO
            try:
                title_tag = job.find("div", class_="title-block").find("a")
                oferta_empleo = title_tag.get_text(strip=True)
            except Exception as e:
                logger.exception(f"Error al extraer oferta de empleo -> {e}")
                oferta_empleo = None

            # EMPRESA
            try:
                empresa_tag = job.find("a", class_="empresa").find("span")
                if empresa_tag:
                    empresa = empresa_tag.get_text()
                else:
                    empresa = "Sin Data"
            except Exception as e:
                logger.exception(f"Error al extraer Empresa de la oferta -> {e}")
                empresa = "Sin Data"

            # LOCALIDAD
            try:
                localidad_tag = job.find("div", class_="info-oferta").find("span", class_="loc").find("span", class_="location").find("span").find("strong")
                if localidad_tag:
                    localidad_provincia = localidad_tag.get_text()
                else:
                    localidad_tag = job.find("div", class_="info-oferta").find("span", class_="loc").find("span", class_="location")
                    if localidad_tag:
                        localidad_provincia = localidad_tag.get_text()
                # CUANDO LA PROVINCIA APARECE DESPUES DE UNA COMA ","
                provincia_tag = job.find("div", class_="info-oferta").find("span", class_="loc").find("span", class_="location").find("span")
                if provincia_tag:
                    complemento_localidad_provincia = provincia_tag.get_text()
                if complemento_localidad_provincia:
                    localidad_provincia = complemento_localidad_provincia

                localidad, provincia = separa_localidad_provincia(localidad_provincia)
            except Exception as e:
                logger.exception(f"Error al extraer provincia/localidad -> {e}")
                localidad = "Sin Data"
                provincia = "Sin Data"

            # FECHA
            try:
                fecha_tag = job.find("div", class_="info-oferta").find("span", class_="fecha")
                if fecha_tag:
                    date_str = fecha_tag.get_text()
                    date_oferta = datetime.strptime(date_str, "%d/%m/%Y").date()
            except Exception as e:
                logger.exception(f"Error al extraer fecha de oferta -> {e}")
                date_oferta = None

            # FOOTER -->  SALARIO - TIPO JORNADA - TIPO CONTRATO
            salary = "Sin Data"
            tipo_jornada = "Sin Data"
            tipo_contrato = 'Sin Data'

            try:
                footer_tag = job.find("p", class_="oi")
                if footer_tag:
                    # Salario
                    salario_tag = footer_tag.find("span", class_="salario")
                    if salario_tag:
                        salary = salario_tag.get_text()
                    # Jornada
                    jornada_tag = footer_tag.find("span", class_="oilast")
                    if jornada_tag:
                        tipo_jornada = jornada_tag.get_text()
                    # Contrato
                    contrato_tag = footer_tag.find("span", class_=lambda x: x is None)
                    if contrato_tag:
                        tipo_contrato = contrato_tag.getText()
            except Exception as e:
                logger.exception(f"Error al extraer salario/tipo de Jornada/tipo de contrato -> {e}")

            encontro_condicion_finalizar = True
            if oferta_empleo and date_oferta == today:
                values = (today, plataforma, provincia, localidad, oferta_empleo, salary, modalidad,
                          tipo_contrato, tipo_jornada, experiencia, empresa, requisitos)
                diccionario = schema_multiple(values=values, keys=keys)
                data.append(diccionario)
                encontro_condicion_finalizar = False

    df = pl.DataFrame(data)
    try:
        df.write_csv(ruta)
        logger.info(f"Scraping finalizado. Datos guardados en: {ruta}")
        return today
    except Exception as e:
        logger.error(f"Error a la hora de cargar -> {e}")
    return ayer


def main(proviene_de_distribuye: bool = False):
    """_summary_
    main: punto de entrada al script dentro de la App tipo Typer.  Inicializa el Logger y controla una serie de intentos
          en caso de que no logre scrapear la pagina web correspondiente en el instante de lanzado el script
          al finalizar lanza el script de `despertar_api` para mantener la Api activa
    """
    logger = get_logger("TrabajoscomScraper")
    carpeta = Path("data/variables")
    ruta_control_ejecusiones = carpeta / "control_ejecusiones.json"

    today = datetime.now().date()
    # limpiar_terminal()
    ultimo_trabajoscom = today - timedelta(days=1)

    intentos = 1
    while intentos < 5 and ultimo_trabajoscom < today:
        logger.info(f"**********   Intento {intentos} de scrapear Trabajoscom  ********************************")
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

        ultimo_trabajoscom = date.fromisoformat(control_ejecusiones["ultima_ejecusion_trabajoscom_scraping"])
        logger.info(f"Ultima vez que se scrapeo Trabajoscom fue {ultimo_trabajoscom}")

        ultimo_trabajoscom = scrapear(logger=logger)
        if ultimo_trabajoscom < today:
            time.sleep(150)
        else:
            control_ejecusiones["ultima_ejecusion_trabajoscom_scraping"] = str(today)
            try:
                guardar_json(archivo=control_ejecusiones, ruta=ruta_control_ejecusiones)
                logger.info(f"Se actualizo la fecha de la ultima vez scraper a --> {today}")
                logger.info("Se ha actualizado el archivo -->  control_ejecusiones.json")
            except Exception as e:
                logger.error(f"Error actualizacon archivo control_ejecusiones.json  -> {e}")

        if intentos == 5:
            logger.info("DESPUES DE 4 INTENTOS NO LOGRO SCRAPEAR TRABAJOSCOM")

    if proviene_de_distribuye:
        ejecutar_script(SCRIPTS_APP["etl"], proviene_de_distribuye=True)
    return
