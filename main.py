import typer
from cli.launcher import lanzar_menu, ejecutar_scraping_randstad, ejecutar_scraping_tecnoempleo, ejecutar_scraping_trabajoscom
from cli.launcher import ejecutar_etl, ejecutar_enviar_api, ejecutar_despertar_api, ejecutar_distribuye_segun_horario

app = typer.Typer(help="Centro de control para scraping, ETL y envío de datos.")


@app.command()
def menu():
    """_summary_
    Funcion de inicio a la App de tipo Typer
    Contiene todos los posibles puntos de entrada a la App
    """
    lanzar_menu()


@app.command()
def randstad():
    ejecutar_scraping_randstad()


@app.command()
def tecnoempleo():
    ejecutar_scraping_tecnoempleo()


@app.command()
def trabajoscom():
    ejecutar_scraping_trabajoscom()


@app.command()
def etl():
    ejecutar_etl()


@app.command()
def enviarapi():
    ejecutar_enviar_api()


@app.command()
def despertarapi():
    ejecutar_despertar_api()


@app.command()
def distribuye():
    ejecutar_distribuye_segun_horario()


if __name__ == "__main__":
    app()
