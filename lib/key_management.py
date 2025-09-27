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


import requests


ENDPOINT_URL = "https://keycheck.searchanalyticsnode.com/"


def is_valid(key):
    try:
        response = requests.get(ENDPOINT_URL, params={"key": key}, timeout=10)

        if response.status_code != 200:
            return False

        if response.json().ok != True:
            return False

        return True

    except Exception as e:
        return False
