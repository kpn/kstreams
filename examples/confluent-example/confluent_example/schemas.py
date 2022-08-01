from schema_registry.client import schema

DEPLOYMENT_AVRO_SCHEMA = {
    "type": "record",
    "namespace": "deployment",
    "name": "Deployment",
    "fields": [
        {"name": "image", "type": "string"},
        {"name": "replicas", "type": "int"},
        {"name": "port", "type": "int"},
    ],
}

COUNTRY_AVRO_SCHEMA = {
    "type": "record",
    "namespace": "country",
    "name": "Country",
    "fields": [{"name": "country", "type": "string"}],
}


deployment_schema = schema.AvroSchema(DEPLOYMENT_AVRO_SCHEMA)
country_schema = schema.AvroSchema(COUNTRY_AVRO_SCHEMA)
