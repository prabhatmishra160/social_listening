import os
import sys
from pathlib import Path
from dotenv import load_dotenv

module = sys.modules[__name__]
COMMON_BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).parent
DEFAULT_ENV_FILE = os.path.join(COMMON_BASE_DIR, ".env")


class VariableNotDefined(Exception):
    pass


class Settings:

    def __init__(self):
        for setting in dir(module):  # iterate through all dynamic variables present in this file.
            if setting.isupper():
                setattr(self, setting, getattr(module, setting, ''))

        for setting in os.environ.items():
            try:
                setattr(self, setting[0], setting[1])
            except Exception:
                pass

        self.load_variables_from_file()

    def __setitem__(self, name, value):
        self.update_variable(name, value)

    def __getattr__(self, item):
        try:
            return self.__getattribute__(item)
        except AttributeError:
            raise VariableNotDefined(f'{item} is not defined.')

    def update_variable(self, name, value):
        os.environ[name] = value
        self.create_variable(name)

    def load_variables_from_file(self, path=DEFAULT_ENV_FILE):

        print('Loading variables from', path)
        load_dotenv(path)

        try:
            with open(path, 'r') as ENV:
                for line in ENV.readlines():
                    if line.strip().strip('\n'):
                        try:
                            name = line.split("=")[0]
                            self.create_variable(name)
                        except Exception as e:  # noqa
                            print(e)

        except Exception as e:  # noqa
            pass

    def create_variable(self, name):
        try:
            var = os.getenv(name)
            setattr(module, name, var)  # updates variables value , e.g BASE_DIR
            setattr(self, name, var)  # update attribute value

        except Exception as e:  # noqa
            print(e)

    def get_base_dir(self, file, parent=True):
        directory = os.path.dirname(os.path.abspath(file))
        if parent:
            directory = os.path.dirname(directory)
        return directory


settings = Settings()
