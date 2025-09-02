from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
import sys
from cli.menus import menu_principal
from scripts_registry import ejecutar_script, SCRIPTS_APP

console = Console()


def lanzar_menu():
    while True:
        menu_principal()
        opcion = Prompt.ask("\n[bold green]Selecciona una opción[/bold green]", choices=["1", "2", "3", "4", "5", "6", "7", "8"], default="8")

        if opcion == "1":
            ejecutar_scraping_randstad()
        elif opcion == "2":
            ejecutar_scraping_tecnoempleo()
        elif opcion == "3":
            ejecutar_scraping_trabajoscom()
        elif opcion == "4":
            ejecutar_etl()
        elif opcion == "5":
            ejecutar_enviar_api()
        elif opcion == "6":
            ejecutar_despertar_api()
        elif opcion == "7":
            ejecutar_despertar_api()
        elif opcion == "8":
            console.print(Panel.fit("👋 Hasta luego, Aliskair!", border_style="red"))
            sys.exit()


def ejecutar_scraping_randstad():
    ejecutar_script(SCRIPTS_APP["randstad"], proviene_de_distribuye=False)
    sys.exit()


def ejecutar_scraping_tecnoempleo():
    ejecutar_script(SCRIPTS_APP["tecnoempleo"], proviene_de_distribuye=False)
    sys.exit()


def ejecutar_scraping_trabajoscom():
    ejecutar_script(SCRIPTS_APP["trabajoscom"], proviene_de_distribuye=False)
    sys.exit()


def ejecutar_etl():
    ejecutar_script(SCRIPTS_APP["etl"], proviene_de_distribuye=False)
    sys.exit()


def ejecutar_enviar_api():
    ejecutar_script(SCRIPTS_APP["enviar_api"])
    sys.exit()


def ejecutar_despertar_api():
    ejecutar_script(SCRIPTS_APP["despertar_api"], maximo_intentos=1, limpiar=True, segundos=10, proviene_de_distribuye=False)
    sys.exit()


def ejecutar_distribuye_segun_horario():
    ejecutar_script(SCRIPTS_APP["distribuye"])
