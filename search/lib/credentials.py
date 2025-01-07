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
            raise PermissionError("Authentication expired. Please rerun the authenticator node.")
        
        # Credentials.from_authorized_user_info raises an error when the refresh_token key
        # does not exist.
        info["refresh_token"] = ""

    return Credentials.from_authorized_user_info(info=info)
