from cleo.application import Application
from src.console.commands import COMMANDS


cli = Application(name = 'Sicredi ETL', version = '0.1.0')
for command in COMMANDS:
    cli.add(command)
