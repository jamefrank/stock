import typer
from stock.utils.custom import my_stock_list

app = typer.Typer(help="Stock analyze commands")

@app.command()
def count():
    typer.echo("Analyze stock count ...")
    my_stock_list()
    pass