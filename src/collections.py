import logging
from typing import Optional, List, Dict, Tuple, Any
from datetime import datetime
import time
import boto3
from botocore.client import Config
from sentinelhub import (
    SHConfig,
    SentinelHubBYOC,
    ByocCollection,
    ByocCollectionAdditionalData,
    ByocCollectionBand,
    ByocTile,
)


class TileListParameters:
    """
    Parameters for listing tiles from an S3 bucket.

    This class holds the parameters needed to connect to and list objects from
    an S3 bucket.
    """

    def __init__(
        self,
        base_path: str,
        bucket_name: str,
        creodias_username: str,
        creodias_password: str,
        bucket_url: Optional[str] = "eodata.dataspace.copernicus.eu",
    ):
        self.bucket_url = bucket_url
        self.base_path = base_path
        self.bucket_name = bucket_name
        self.creodias_username = creodias_username
        self.creodias_password = creodias_password


class Ingestor:
    """
    Handles the ingestion of data into a BYOC (Bring Your Own COG) collection.

    This class manages the entire workflow of creating a collection, listing files,
    building tile information, and ingesting the data into Sentinel Hub.
    """

    def __init__(self, config: SHConfig):
        """
        Initialize the Ingestor with a Sentinel Hub configuration.

        Args:
            sentinelhub.config.SHConfig: A SentinelHub configuration object with authentication details
        """
        self.byoc_client = self.initialise_byoc_client(config)
        self.byoc_collection = None
        self.file_list = []

    def initialise_byoc_client(self, config: SHConfig) -> SentinelHubBYOC:
        """
        Create and return a SentinelHubBYOC client.

        Args:
            sentinelhub.config.SHConfig: A SentinelHub configuration object with authentication details

        Returns:
            sentinelhub.api.byoc.SentinelHubBYOC: A client for interacting with the BYOC API
        """
        return SentinelHubBYOC(config=config)

    def create_byoc_collection(
        self,
        collection_name: str,
        bucket_name: str,
        storage_id: Optional[str] = None,
        band_information: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Create a new BYOC collection in Sentinel Hub.

        This function creates a new collection with the specified name and associates
        it with the given S3 bucket. Optionally, a storage identifier can be provided.

        Args:
            collection_name: The name for the new collection
            bucket_name: The S3 bucket name where data is stored
            storage_id: Optional storage identifier (e.g., "eodata" for CDSE)

        Returns:
            None: The created collection is stored in self.byoc_collection
        """
        if band_information is not None:
            # Define the bands
            band_parameters = {}
            # Define the required fields
            required_fields = ["name", "source", "bit_depth", "sample_format"]

            # Check for required fields in each band
            for band in band_information:
                missing_fields = [
                    field for field in required_fields if field not in band
                ]
                if missing_fields:
                    raise ValueError(
                        f"Each band must contain {', '.join(missing_fields)} fields"
                    )
                # Build the band parameters
                band_parameters[band["name"]] = ByocCollectionBand(
                    source=band["source"],
                    band_index=1,
                    bit_depth=band["bit_depth"],
                    sample_format=band["sample_format"],
                )
            if storage_id:
                band_config = ByocCollectionAdditionalData(
                    bands=band_parameters, other_data={"storageIdentifier": storage_id}
                )
            else:
                band_config = ByocCollectionAdditionalData(
                    bands=band_parameters,
                )
        else:
            if storage_id:
                band_config = ByocCollectionAdditionalData(
                    other_data={"storageIdentifier": storage_id}
                )
            else:
                band_config = None

        new_collection = ByocCollection(
            name=collection_name, s3_bucket=bucket_name, additional_data=band_config
        )

        self.byoc_collection = self.byoc_client.create_collection(new_collection)
        time.sleep(5)

    def list_tiles(self, params: TileListParameters) -> List[str]:
        """
        List all TIFF files in the S3 bucket that match the given parameters.

        This function connects to an S3 bucket, recursively searches for all TIFF files
        (.tif or .tiff) within the specified base path, and stores them in self.file_list.

        Args:
            params: TileListParameters object containing S3 access information

        Returns:
            List[str]: The list of file paths found (also stored in self.file_list)
        """
        # Create a session and client
        session = boto3.session.Session()
        s3_client = session.client(
            "s3",
            endpoint_url=f"https://{params.bucket_url}",
            aws_access_key_id=params.creodias_username,
            aws_secret_access_key=params.creodias_password,
            config=Config(signature_version="s3v4"),
            verify=True,  # Check SSL certificate
        )

        # Use a paginator to handle large result sets
        paginator = s3_client.get_paginator("list_objects_v2")
        operation_parameters = {
            "Bucket": params.bucket_name,
            "Prefix": params.base_path,
        }
        tiff_files = []
        for page in paginator.paginate(**operation_parameters):
            if "Contents" in page:
                for obj in page["Contents"]:
                    # Get all subfolders
                    for subpages in paginator.paginate(
                        Bucket=params.bucket_name, Prefix=obj["Key"]
                    ):
                        if "Contents" in subpages:
                            for subobj in subpages["Contents"]:
                                if subobj["Key"].endswith(
                                    (".tiff", ".tif", ".TIF", ".TIFF")
                                ):
                                    tiff_files.append(subobj["Key"])

        self.file_list = tiff_files
        return tiff_files

    def build_byoc_tiles(
        self, sensing_time_position: Dict[str, Any], band_position: Dict[str, Any]
    ) -> List[Tuple[str, datetime]]:
        """
        Build BYOC tile from file paths.

        This function processes the file paths in self.file_list to extract datetime
        information and create tile paths with (BAND) placeholders that Sentinel Hub uses
        to identify the different bands of each tile.

        Args:
            sensing_time_position: Dictionary with parameters for extracting datetime:
                - path: Position of folder containing datetime in path
                - delimiter: Character separating parts of the folder name
                - position: Position of datetime part after splitting
                - format: Format string for datetime parsing (e.g., "%Y%m%d")
            band_position: Dictionary with parameters for identifying bands:
                - path: Position of filename in path (-1 for last element)
                - delimiter: Character separating parts of the filename
                - position: Position of band identifier after splitting

        Returns:
            List[Tuple[str, datetime]]: List of tuples containing:
                - Tile path with (BAND) placeholder
                - Datetime object representing sensing time
        """
        byoc_tiles = []
        for tile_path in self.file_list:
            # Get the sensing time from the tile path
            folder_name = tile_path.split("/")[sensing_time_position["path"]]
            datetime_str = folder_name.split(sensing_time_position["delimiter"])[
                sensing_time_position["position"]
            ]
            datetime_obj = datetime.strptime(
                datetime_str, sensing_time_position["format"]
            )
            file_name = tile_path.split("/")[band_position["path"]]
            split_file_name = file_name.split(band_position["delimiter"])
            split_file_name[band_position["position"]] = "(BAND)"
            new_file_name = band_position["delimiter"].join(split_file_name)
            parent_path = tile_path.split("/")[0:-1]
            byoc_path = f"{'/'.join(parent_path)}/{new_file_name}"

            if not byoc_path in [x[0] for x in byoc_tiles]:
                byoc_tiles.append([byoc_path, datetime_obj])

        return byoc_tiles

    def ingest_tiles_to_collection(
        self, sensing_time_position: Dict[str, Any], band_position: Dict[str, Any]
    ) -> None:
        """
        Ingest tiles into the BYOC collection.

        This function creates tiles in the Sentinel Hub BYOC collection based on the
        file paths discovered in the S3 bucket. It checks for existing tiles to avoid
        duplicates and only ingests new tiles.

        Args:
            sensing_time_position: Dictionary with parameters for extracting datetime
            band_position: Dictionary with parameters for identifying bands

        Returns:
            None

        Note:
            The collection must be created first (using create_byoc_collection)
            Tiles are created in Sentinel Hub but actual data ingestion happens asynchronously
        """
        byoc_tiles = self.build_byoc_tiles(sensing_time_position, band_position)
        existing_tiles = list(self.byoc_client.iter_tiles(self.byoc_collection))
        for tile in byoc_tiles:
            byoc_tile = ByocTile(path=tile[0], sensing_time=tile[1])
            if byoc_tile.path not in [x.path for x in existing_tiles]:
                self.byoc_client.create_tile(self.byoc_collection, byoc_tile)

    def collection_tile_report(self) -> Tuple[Dict[str, int], List[str]]:
        """
        Generate a report of the tile ingestion status in the collection.

        Returns:
            Tuple[Dict[str, int], List[str]]: A tuple containing:
                - A dictionary with counts of tiles by status (Ingested, Failed, Pending, Total)
                - A list of failure reasons for failed tiles
        """
        tiles = list(self.byoc_client.iter_tiles(self.byoc_collection))

        report = {"Ingested": 0, "Failed": 0, "Pending": 0, "Total": len(tiles)}
        failed = []

        for tile in tiles:
            if tile["status"] == "INGESTED":
                report["Ingested"] += 1
            elif tile["status"] == "FAILED":
                report["Failed"] += 1
                # Safely extract failure cause if available
                if (
                    "additionalData" in tile
                    and "failedIngestionCause" in tile["additionalData"]
                ):
                    failed.append(tile["additionalData"]["failedIngestionCause"])
                else:
                    failed.append(f"Unknown failure for tile {tile['path']}")
            else:
                report["Pending"] += 1

        return report, failed
