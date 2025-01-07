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


def get_schema(dialog_creation_context):
    import knime.extension as knext

    availableProps = []
    if "available_props" in dialog_creation_context.flow_variables:
        availableProps = dialog_creation_context.flow_variables.get("available_props")

    columns = []
    for prop in availableProps:
        # According to the function signature of knext.Column(), the metadata parameter is optional.
        # However, when setting it to None or an empty dict, an error gets written to the log file
        # which states the keys preferred_value_type and displayed_column_type are missing.
        # Therefore these keys are specified with empty values.
        columns.append(
            knext.Column(
                ktype=knext.string(),
                name=prop,
                metadata={"preferred_value_type": "", "displayed_column_type": ""}
            )
        )
    
    return knext.Schema.from_columns(columns=columns)


def create():
    import knime.extension as knext

    return knext.ColumnParameter(
        label="Property",
        description="Select one of your verified properties. Requires the Search Analytics Authenticator node to be connected and executed.",
        schema_provider=get_schema
    )
