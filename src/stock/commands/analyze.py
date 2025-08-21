import typer
from stock.utils.custom import my_stock_list

app = typer.Typer(help="Stock analyze commands")

@app.command()
def select():
    typer.echo("Analyze stock ...")

    pass