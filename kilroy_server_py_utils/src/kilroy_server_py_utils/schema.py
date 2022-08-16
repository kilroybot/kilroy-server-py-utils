from typing import Any, Dict

from jsonschema.exceptions import SchemaError
from jsonschema.validators import Draft202012Validator
from pydantic import BaseModel, Extra, root_validator


class JSONSchema(BaseModel, extra=Extra.allow):
    __root__: Dict[str, Any]

    @root_validator(pre=True)
    def validate(cls, values) -> Dict[str, Any]:
        try:
            Draft202012Validator.check_schema(values)
        except SchemaError as e:
            raise ValueError(
                "Schema is not a valid JSON Schema 2020-12."
            ) from e
        if "type" not in values:
            raise ValueError("Schema should have a type field.")
        elif values["type"] != "object":
            raise ValueError("Only object types are allowed.")
        return {"__root__": values}
