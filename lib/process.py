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


import psutil


def terminate_tree(pid):
    parent = psutil.Process(pid=pid)
    children = parent.children(recursive=True)

    tree = [parent, *children]

    for e in tree:
        try:
            e.terminate()
        except psutil.NoSuchProcess:
            pass

    _, alive = psutil.wait_procs(procs=tree, timeout=2)

    for e in alive:
        try:
            e.kill()
        except psutil.NoSuchProcess:
            pass
