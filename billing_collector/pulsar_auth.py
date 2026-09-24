import logging
import os
from collections.abc import Callable

import pulsar


def _read_token_file(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read().strip()


def _token_file_supplier(path: str) -> Callable[[], str]:
    def supplier() -> str:
        # The Pulsar client calls this on every new connection and on every auth challenge from
        # the broker, so a rotated token is picked up without a restart. It must not raise: an
        # exception escaping into the C++ client during an auth challenge can abort the process.
        # An empty token makes the broker reject the connection and the client retries later,
        # reading the file again.
        try:
            token = _read_token_file(path)
        except Exception as e:
            logging.error(f"Could not read Pulsar token file {path}: {e}")
            return ""

        if not token:
            logging.error(f"Pulsar token file {path} is empty")

        return token

    return supplier


def pulsar_authentication() -> pulsar.Authentication | None:
    """
    Returns the Pulsar authentication configured by the environment, for passing as
    `authentication=` to `pulsar.Client`:

      - PULSAR_TOKEN_FILE: path to a file holding a JWT. The file is read again every time the
        client needs the token, so the mounted Secret can be rotated in place.
      - PULSAR_TOKEN: the JWT itself. Ignored if PULSAR_TOKEN_FILE is set.
      - Neither set: None, ie no authentication.

    If PULSAR_TOKEN_FILE is set but the file can't be read or is empty then this raises, so a
    misconfigured deployment fails at startup rather than with authentication errors later.

    This matches eodhp_utils.runner.pulsar_authentication from eodhp-utils v0.1.16.
    """
    token_file = os.environ.get("PULSAR_TOKEN_FILE")
    if token_file:
        if not _read_token_file(token_file):
            raise ValueError(f"Pulsar token file {token_file} is empty")

        logging.info(f"Using Pulsar token authentication from {token_file}")
        return pulsar.AuthenticationToken(_token_file_supplier(token_file))

    token = os.environ.get("PULSAR_TOKEN", "").strip()
    if token:
        logging.info("Using Pulsar token authentication from PULSAR_TOKEN")
        return pulsar.AuthenticationToken(token)

    return None
