from typing import Any

from elasticsearch import Elasticsearch

# Define index mapping
default_mapping = {
    "mappings": {
        "properties": {
            "type": {"type": "keyword"},
            "amenity": {"type": "text"},
            "name": {"type": "text"},
            "hex_id": {"type": "text"},  # For hexagon centers
            "location": {"type": "geo_point"},  # For POIs (Points of Interest)
            "polygon": {"type": "geo_shape"},  # For district shapes (GeoShapes)
        }
    }
}


class ElasticsearchIndexManager:
    def __init__(
        self,
        es_client: Elasticsearch,
    ) -> None:
        self.es = es_client

    def create_index(
        self,
        index_name: str,
        ignore_if_exists: bool = False,
        mapping: dict[str, Any] = default_mapping,
    ) -> None:
        if self.es.indices.exists(index=index_name):
            msg = f'Index "{index_name}" already exists.'
            if ignore_if_exists:
                print("Warning:", msg)
            else:
                raise ValueError(msg)
        else:
            self.es.indices.create(index=index_name, body=mapping)

    def delete_index(
        self,
        index_name: str,
        ignore_if_not_exists: bool = True,
    ) -> None:
        if self.es.indices.exists(index=index_name):
            self.es.indices.delete(index=index_name)
        else:
            msg = f'Index "{index_name}" doesn\'t exist.'
            if ignore_if_not_exists:
                print("Warning:", msg)
            else:
                raise ValueError(msg)
