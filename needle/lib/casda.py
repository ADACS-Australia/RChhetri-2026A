import os
import keyring
from keyring.backend import KeyringBackend

from astroquery.casda import Casda


class _EnvKeyring(KeyringBackend):
    """Serves the CASDA password from the environment; stores nothing."""

    priority: float = 10

    def get_password(self, service, username):
        return os.environ.get("CASDA_PASSWORD")

    def set_password(self, service, username, password):
        raise NotImplementedError("Environment keyring is read-only")

    def delete_password(self, service, username):
        raise NotImplementedError("Environment keyring is read-only")


def casda_login() -> Casda:
    username = os.environ.get("CASDA_USERNAME")

    if not username or not os.environ.get("CASDA_PASSWORD"):
        raise RuntimeError("""A CASDA username and password is required for downloading.
            Set environment variables CASDA_USERNAME and CASDA_PASSWORD.""")

    keyring.set_keyring(_EnvKeyring())

    casda = Casda()
    casda.login(username=username)

    return casda
