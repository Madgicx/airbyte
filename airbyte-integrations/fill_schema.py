import argparse
import json
from pathlib import Path


def fill_schema_types_from_existing(schema_file_path: str, schemas_folder_path: str):
    with open(schema_file_path, "r+") as schema_file:
        schema_json = json.loads(schema_file.read())
        properties = schema_json["properties"]

        for existing_schema in Path(schemas_folder_path).iterdir():
            if str(existing_schema) != schema_file_path and existing_schema.is_file() and str(existing_schema).endswith(".json"):
                with open(existing_schema, "r+") as existing_schema_file:
                    existing_schema_json = json.loads(existing_schema_file.read())
                    existing_properties = existing_schema_json.get("properties", [])

                    for property_name, property_value in properties.items():
                        if property_value:
                            continue

                        if property_name in existing_properties:
                            properties[property_name] = existing_properties[property_name]

        for property_name, property_value in properties.items():
            if property_value:
                continue

            properties[property_name] = {"type": []}

        schema_file.truncate(0)
        schema_file.seek(0)
        schema_file.write(json.dumps(schema_json, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="My Parser")
    parser.add_argument(
        "--schema_file",
        dest="schema_file", 
        type=str,
        help="Path to the schema file (e.g.:airbyte-integrations/connectors/source-google-ads/source_google_ads/schemas/customer.json)."
    )
    parser.add_argument(
        "--schemas_folder", 
        dest="schemas_folder",
        type=str,
        help="Path to the schemas folder (e.g.:airbyte-integrations/connectors/source-google-ads/source_google_ads/schemas)."
    )
    args = parser.parse_args()

    fill_schema_types_from_existing(args.schema_file, args.schemas_folder)
