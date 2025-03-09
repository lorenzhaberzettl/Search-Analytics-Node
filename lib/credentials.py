# Copyright 2024 Vitus Haberzettl
# Copyright 2024, 2025 Lorenz Haberzettl
#
#
# This file is part of Search Analytics Node.
#
# Search Analytics Node is free software: you can redistribute it and/or modify it under the terms
# of the GNU General Public License as published by the Free Software Foundation, either version 3
# of the License, or (at your option) any later version.
#
# Search Analytics Node is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# Search Analytics Node. If not, see <https://www.gnu.org/licenses/>.


import logging
from multiprocessing import Process, Queue
from random import randint
import socket
import time

from google_auth_oauthlib.flow import InstalledAppFlow

import lib.process


logger = logging.getLogger(__name__)


OAUTH_CLIENT_CONFIG = {
    "installed": {
        "client_id": "",
        "project_id": "",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": "",
    }
}

GOOGLE_API_SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


def parse_json(credentials_json):
    import json
    from datetime import datetime, timezone

    from google.oauth2.credentials import Credentials

    info = json.loads(credentials_json)

    if "refresh_token" not in info:
        # Make sure the credentials have not expired.
        time_expiry = datetime.fromisoformat(info["expiry"])
        time_now = datetime.now(tz=timezone.utc)
        if (time_expiry.timestamp() - time_now.timestamp()) <= 0:
            raise PermissionError(
                "Authentication expired. Please execute the Authenticator Node again."
            )

        # Credentials.from_authorized_user_info raises an error when the refresh_token key
        # does not exist.
        info["refresh_token"] = ""

    return Credentials.from_authorized_user_info(info=info)


def create_new(exec_context):
    flow = InstalledAppFlow.from_client_config(
        client_config=OAUTH_CLIENT_CONFIG, scopes=GOOGLE_API_SCOPES
    )

    queue = Queue()

    childProcess = Process(
        target=_run_local_server, kwargs={"queue": queue, "flow": flow}
    )
    childProcess.start()

    while True:
        if exec_context.is_canceled() == True:
            if childProcess.pid is not None:
                lib.process.terminate_tree(childProcess.pid)
            raise RuntimeError("Execution was canceled.")

        if childProcess.is_alive() == False:
            break

        time.sleep(0.5)

    childProcess.join()
    if childProcess.exitcode != 0:
        raise RuntimeError(
            "Subprocess exit code was non zero: " + str(childProcess.exitcode)
        )

    return parse_json(queue.get(block=False))


def _run_local_server(queue, flow):
    try:
        credentials = flow.run_local_server(
            host="127.0.0.1",
            port=_get_free_port(),
            authorization_prompt_message=None,
            success_message="Authorized successfully.\n\nRevoke access of this application to your Google Account anytime at https://myaccount.google.com/connections\n\nHeads up! Your authentication details are saved in your workflow. If you share a workflow with an executed Authenticator node, anyone who has access to the workflow can use it to run queries on your Google Search Console properties.\n\nYou can close this window now.",
            open_browser=True,
            timeout_seconds=None,
        )
        queue.put(credentials.to_json())
    except Exception as e:
        # When a subprocess raises an exception, it is not visible inside the KNIME log file. Only
        # exceptions in the main process are logged. We, therefore, manually log it.
        logger.error(str(e))
        raise

    queue.close()


def _get_free_port():
    for i in range(0, 100):
        port = randint(10000, 30000)
        if _is_port_free(port=port) == True:
            return port

    raise RuntimeError("Can not find free port on loopback interface.")


def _is_port_free(port):
    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False
