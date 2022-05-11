from cleo.commands.command import Command
from cleo.helpers import option
from pathlib import Path
from src.pipelines.user_etl import user_etl
from src.pipelines.prepare import prepare_environment


class EtlCommand(Command):
    """
    Executes the ETL pipeline and saves the result as CSV in a directory specified by the user.

    etl
    """

    options = [
        option('directory', None, 'Directory to save the CSV file', flag = False, value_required = True)
    ]

    help = '''\
Executes the ETL pipeline and saves the result as CSV in a directory specified by the user.
'''

    def handle(self):
        directory = self.option('directory')

        if not directory:
            directory = Path(
                f"{Path.cwd()}/src/data/flat/"
            )

            friendly_directory = Path(
                f"{Path.cwd().name}/src/data/flat/"
            )

        question = self.create_question(
            f'Directory to save the file [<comment>{friendly_directory}</comment>]: ',
            default = directory
        )

        directory = Path(
            self.ask(question)
        )

        # Prepare
        prepare_environment()

        # Writes
        user_etl(
            output_dir = Path(directory)
        )
