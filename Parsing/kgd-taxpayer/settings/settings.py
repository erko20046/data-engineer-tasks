import json

def get_config() -> dict:
    with open('config.json') as file:
        config = dict(json.load(file))
        return config

config_file = get_config()

class Configurations:
    port: str = config_file['parser']['port']
    login: str = config_file['parser']['login']
    password: str = config_file['parser']['password']

configurations = Configurations()
