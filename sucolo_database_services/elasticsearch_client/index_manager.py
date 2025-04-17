from typing import Any

from elasticsearch import Elasticsearch

# Define index mapping
default_mapping = {
    "mappings": {
        "properties": {
            # Shared properties
            "type": {"type": "keyword"},
            # PoI properties
            "amenity": {"type": "text"},
            "location": {"type": "geo_point"},  # For POIs (Points of Interest)
            # Hexagon properties
            "hex_id": {"type": "text"},  # For hexagon centers
            # District properties
            "district": {"type": "text"},
            "polygon": {"type": "geo_shape"},  # For district shapes (GeoShapes)
            # Hexagon and district properties
            "Average age": {"type": "float"},
            "Employed income": {"type": "float"},
            "Gross monthly wages": {"type": "float"},
            "Household income": {"type": "float"},
            "Households with 1 person": {"type": "float"},
            "Households with 2 people": {"type": "float"},
            "Households with 3 people": {"type": "float"},
            "Households with 4 people": {"type": "float"},
            "Households with 5 or more people": {"type": "float"},
            "Housing": {"type": "float"},
            "Other income": {"type": "float"},
            "Pensions": {"type": "float"},
            "Population density": {"type": "float"},
            "Total employed": {"type": "float"},
            "Total population": {"type": "float"},
            "Total unemployed": {"type": "float"},
            "Unemployment benefits": {"type": "float"},
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
        ignore_if_index_not_exist: bool = True,
    ) -> None:
        if self.es.indices.exists(index=index_name):
            self.es.indices.delete(index=index_name)
        else:
            msg = f'Index "{index_name}" doesn\'t exist.'
            if ignore_if_index_not_exist:
                print("Warning:", msg)
            else:
                raise ValueError(msg)

    def index_exists(self, index_name: str) -> bool:
        return self.es.indices.exists(  # type: ignore[return-value]
            index=index_name
        )
