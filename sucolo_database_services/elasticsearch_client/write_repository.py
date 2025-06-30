from typing import Any, Iterator

import geopandas as gpd
from elasticsearch import Elasticsearch, helpers

from sucolo_database_services.utils.polygons2hexagons import polygons2hexagons


class ElasticsearchWriteRepository:
    def __init__(self, es_client: Elasticsearch):
        self.es = es_client

    def upload_pois(
        self,
        index_name: str,
        gdf: gpd.GeoDataFrame,
        extra_features: list[str] = [],
    ) -> None:
        def doc_stream() -> Iterator[dict[str, Any]]:
            if len(extra_features) > 0:
                pois_features = gdf[extra_features].to_dict()
            else:
                pois_features = {}
            for amenity, point in zip(gdf["amenity"], gdf["geometry"]):
                data = {
                    "type": "poi",
                    "amenity": amenity,
                    "location": {"lon": point.x, "lat": point.y},
                }
                data.update(pois_features)
                yield data

        for status_ok, response in helpers.streaming_bulk(
            self.es,
            actions=doc_stream(),
            chunk_size=1000,
            index=index_name,
        ):
            if not status_ok:
                print(response)

    def upload_districts(
        self,
        index_name: str,
        gdf: gpd.GeoDataFrame,
    ) -> None:
        """Upload districts to Elasticsearch.

        Args:
            gdf (gpd.GeoDataFrame): GeoDataFrame containing district polygons
        """
        gdf["polygon"] = gdf["geometry"].apply(lambda g: g.wkt)
        gdf = gdf[["district", "polygon"]]

        print(f"Prepared {len(gdf)} documents for upload")

        try:
            for i, row in gdf.iterrows():
                try:
                    self.es.index(
                        index=index_name,
                        id=row["district"],
                        document=row.to_dict(),
                    )
                    if i % 10 == 0:  # Log progress every 10 documents
                        print(f"Uploaded {i+1}/{len(gdf)} districts")
                except Exception as e:
                    print(
                        f"Error uploading district {row['district']}: {str(e)}"
                    )

            print("Upload completed")
            # Verify the upload
            count = self.es.count(index=index_name)
            print(f"Total documents in index: {count['count']}")

        except Exception as e:
            print(f"Error during upload: {str(e)}")

    def upload_hex_centers(
        self,
        index_name: str,
        districts: gpd.GeoDataFrame,
        hex_resolution: int,
    ) -> None:
        distric_hexagons = polygons2hexagons(
            districts, resolution=hex_resolution
        )
        districts = districts.drop(columns=["district", "geometry"])

        def doc_stream() -> Iterator[dict[str, Any]]:
            for distric_id, hex_centers in distric_hexagons.items():
                district_features = districts.loc[distric_id].to_dict()
                for id_, center in hex_centers:
                    data = {
                        "type": "hex_center",
                        "hex_id": id_,
                        "reslution": hex_resolution,
                        "location": {"lon": center.x, "lat": center.y},
                    }
                    data.update(district_features)
                    yield data

        for status_ok, response in helpers.streaming_bulk(
            self.es,
            actions=doc_stream(),
            chunk_size=1000,
            index=index_name,
        ):
            if not status_ok:
                print(response)
