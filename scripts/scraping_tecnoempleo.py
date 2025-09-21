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


def separa_provincia_modalidad_fecha_salario(cadena: str) -> tuple[str, str, date, str]:
    remoto = False
    tiene_parentesis = False
    hoy = datetime.now().date()
    if "100% remoto" in cadena:
        remoto = True
        modalidad = "100% remoto"
        provincia = "Sin Data"

    index_separar_bloques = cadena.find("-")
    bloque_1 = cadena[0:index_separar_bloques]
    bloque_2 = cadena[index_separar_bloques + 1:]

    try:
        date_str = bloque_2.strip()[0:10]
        date_oferta = datetime.strptime(date_str, "%d/%m/%Y").date()
        bloque3 = bloque_2[11:].replace("Nueva", "").replace("Actualizada", "").strip()
        salary = bloque3 if len(bloque3) > 1 else "Sin Data"
    except Exception:
        return ("Sin Data", "Sin Data", hoy, "Sin Data")

    if not remoto:
        if "(" in bloque_1 and ")" in bloque_1:
            tiene_parentesis = True

        if not tiene_parentesis:
            provincia = bloque_1.strip()
            modalidad = "Sin Data"
        else:
            index_ini = bloque_1.find("(")
            index_fin = bloque_1.find(")")
            provincia = bloque_1[0: index_ini]
            modalidad = bloque_1[index_ini + 1: index_fin]

    return (provincia, modalidad, date_oferta, salary)


def scrapear(logger: logging) -> date:
    """_summary_
    scrapear: Esta función scrapea la pagina web Tecnoempleo en busca de los empleos publicados el dia de hoy

    Args:
        logger (logging): Recibe el Logger de la pagina para llevar los registros con el mismo formato en el mismo archivo

    Returns:
        date: Retorna la fecha de hoy (today) si logró scrapear la pagina correctamente, de lo contrario fue que no lo consiguió
    """

    today = datetime.now(ZoneInfo("America/Caracas")).date()
    ayer = today - timedelta(days=1)
    encontro_algo_de_data = False

    plataforma = 'Tecnoempleo'
    localidad = 'Sin Data'
    experiencia = 'Sin Data'
    tipo_contrato = 'Sin Data'
    tipo_jornada = 'Sin Data'

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
            url = "https://www.tecnoempleo.com/ofertas-trabajo/"
        else:
            url = "https://www.tecnoempleo.com/ofertas-trabajo/?pagina=" + str(page)
            time.sleep(30)

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.warning(f"No se pudo cargar la pagina {page} - URL: {url}")
            time.sleep(60)
            continue

        logger.info(f"Pagina {page} cargada correctamente - URL: {url}")
        page += 1
        soup = BeautifulSoup(response.text, "html.parser")

        job_container = soup.find("div", class_="col-12 col-sm-12 col-md-12 col-lg-9")
        jobs = job_container.find_all("div", recursive=False)

        for job in jobs:
            body_tag = job.find("div", class_="col-10 col-md-9 col-lg-7")
            try:
                # OFDERTA DE EMPLEO - EMPRESA
                try:
                    h3_tag = body_tag.find("h3")
                    oferta_tag = h3_tag.find("a")
                    oferta_empleo = oferta_tag.get_text(strip=True)
                except Exception as e:
                    logger.exception(f"Error al extraer oferta de empleo -> {e}")
                    oferta_empleo = "Sin Data"

                try:
                    empresa_tag = body_tag.find("a", class_="text-primary link-muted")
                    if empresa_tag:
                        empresa = empresa_tag.get_text(strip=True)
                    else:
                        empresa = "Sin Data"
                except Exception as e:
                    logger.exception(f"Error al extraer Empresa de la oferta -> {e}")
                    empresa = 'Sin Data'

                # PROVINCIA - MODALIDAD - FECHA - SALARIO
                try:
                    provincia_tag = body_tag.find("span", class_="d-block d-lg-none text-gray-800")
                    if provincia_tag:
                        provincia_modalidad_fecha_salario = provincia_tag.get_text(strip=True)
                        provincia, modalidad, date_oferta, salary = separa_provincia_modalidad_fecha_salario(provincia_modalidad_fecha_salario)
                    else:
                        provincia, modalidad, date_oferta, salary = ("Sin Data", "Sin Data", today, "Sin Data")
                except Exception as e:
                    logger.exception(f"Error al extraer Provincia, Modalidad, Salario, Fecha de la oferta -> {e}")
                    provincia = 'Sin Data'
                    modalidad = 'Sin Data'
                    salary = 'Sin Data'
                    date_oferta = None

                # REQUISITOS
                requisitos_tag = body_tag.find("span", class_="hidden-md-down text-gray-800")
                requisitos = ""
                try:
                    if requisitos_tag:
                        span_requisitos = requisitos_tag.find_all("span")
                        total_requisitos = len(span_requisitos)
                        for i, span in enumerate(span_requisitos):
                            if i + 1 == total_requisitos:
                                requisitos = requisitos + span.get_text(strip=True)
                            else:
                                requisitos = requisitos + span.get_text(strip=True) + " -"
                        requisitos = requisitos.strip()
                    else:
                        requisitos = "Sin Data"
                except Exception as e:
                    requisitos = "Sin Data"
                    logger.exception(f"Error al extraer Requisitos de la oferta -> {e}")
            except Exception as e:
                logger.exception(f"Error general en el procesamiento de la pagina {page} -> {e}")
                pass

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
    logger = get_logger("TecnoempleoScraper")
    carpeta = Path("data/variables")
    ruta_control_ejecusiones = carpeta / "control_ejecusiones.json"

    today = datetime.now(ZoneInfo("America/Caracas")).date()
    # limpiar_terminal()
    ultimo_tecnoempleo = today - timedelta(days=1)
    encontro_algo_de_data = False

    intentos = 1
    while intentos < 5 and ultimo_tecnoempleo < today and (intentos == 1 or encontro_algo_de_data):
        logger.info(f"************** Intento {intentos} de scrapear Tecnoempleo  ********************************")
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

        ultimo_tecnoempleo = date.fromisoformat(control_ejecusiones["ultima_ejecusion_tecnoempleo_scraping"])
        logger.info(f"Ultima vez que se scrapeo Tecnoempleo fue {ultimo_tecnoempleo}")

        ultimo_tecnoempleo, encontro_algo_de_data = scrapear(logger=logger)
        if ultimo_tecnoempleo < today and encontro_algo_de_data:
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
                logger.error(f"Error actualizacon archivo -->  control_ejecusiones.json -> {e}")

        if intentos == 5:
            logger.info("DESPUES DE 4 INTENTOS NO LOGRO SCRAPEAR TECNOEMPLEO")

    if proviene_de_distribuye:
        ejecutar_script(SCRIPTS_APP["trabajoscom"], proviene_de_distribuye=True)
    return
