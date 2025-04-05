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
        index_name: str,
        mapping: dict[str, Any] = default_mapping,
    ) -> None:
        self.es = es_client
        self.index_name = index_name
        self.mapping = mapping

    def create_index(
        self,
        ignore_if_exists: bool = False,
    ) -> None:
        if self.es.indices.exists(index=self.index_name):
            msg = f'Index "{self.index_name}" already exists.'
            if ignore_if_exists:
                print("Warning:", msg)
            else:
                raise ValueError(msg)
        else:
            self.es.indices.create(index=self.index_name, body=self.mapping)

    def delete_index(
        self,
        ignore_if_not_exists: bool = True,
    ) -> None:
        if self.es.indices.exists(index=self.index_name):
            self.es.indices.delete(index=self.index_name)
        else:
            msg = f'Index "{self.index_name}" doesn\'t exist.'
            if ignore_if_not_exists:
                print("Warning:", msg)
            else:
                raise ValueError(msg)
