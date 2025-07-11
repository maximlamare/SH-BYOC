from sentinelhub import SHConfig


class Configurator:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.config = self.get_config()

    def get_config(self) -> SHConfig:
        config = SHConfig()
        config.sh_client_id = self.client_id
        config.sh_client_secret = self.client_secret
        config.sh_base_url = "https://sh.dataspace.copernicus.eu"
        config.sh_token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        return config
