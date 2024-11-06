import os

class EnvLoader:
    def __init__(self, env_path='./.env'):
        self.env_path = env_path

    def load_env(self):
        with open(self.env_path, 'r') as file:
            for line in file:
                if line.strip() and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value

    def get_variable(self, name):
        return os.environ.get(name)
