import contextlib
import warnings

import requests
from urllib3.exceptions import InsecureRequestWarning


@contextlib.contextmanager
def no_ssl_verification():
    """
    ```py
    with no_ssl_verification():
        # download stuff
    ```
    """

    def merge_environment_settings(self, url, proxies, stream, verify, cert):
        opened_adapters.add(self.get_adapter(url))
        settings = original_merge_environment_settings(self, url, proxies, stream, verify, cert)
        settings["verify"] = False
        return settings

    opened_adapters = set()
    original_merge_environment_settings = requests.Session.merge_environment_settings
    requests.Session.merge_environment_settings = merge_environment_settings
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", InsecureRequestWarning)
            yield
    finally:
        requests.Session.merge_environment_settings = original_merge_environment_settings
        for adapter in opened_adapters:
            try:
                adapter.close()
            except Exception:
                pass
