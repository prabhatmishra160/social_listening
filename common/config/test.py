from dynaconf import settings

settings.load_file(path="./config.toml")
print(list(settings))
USERNAME = settings.USERNAME