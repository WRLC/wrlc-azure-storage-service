"""Storage Helpers for Azure Blob and Queue Services"""

import json
import logging
from azure.core.paging import ItemPaged
from typing import Dict, List, Union, Optional, Any

from azure.data.tables import TableServiceClient, UpdateMode, TableClient, TableEntity
from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings,
    BlobClient,
    ContainerClient,
    BlobProperties,
)
from azure.storage.queue import QueueServiceClient, QueueClient, TextBase64EncodePolicy
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

from wrlc_azure_storage_service.config import STORAGE_CONNECTION_STRING


# noinspection PyMethodMayBeStatic
class StorageService:
    """Service for Azure Storage operations."""

    def __init__(self, storage_connection_string: Optional[str] = None) -> None:
        """
        Initializes the StorageService.
        Args:
            storage_connection_string: The Azure Storage connection string.
                                       If None, it's retrieved from the
                                       STORAGE_CONNECTION_STRING env var.
        """
        self.connection_string = storage_connection_string or STORAGE_CONNECTION_STRING
        if not self.connection_string:
            raise ValueError(
                "Storage connection string not found. "
                "Please provide it or set the STORAGE_CONNECTION_STRING environment variable."
            )

    def get_blob_service_client(self) -> BlobServiceClient:
        """Returns an authenticated BlobServiceClient instance."""
        try:
            return BlobServiceClient.from_connection_string(self.connection_string)
        except ValueError as e:
            logging.error(
                msg=f"StorageService.get_blob_service_client: Invalid storage connection string format: {e}"
            )
            raise ValueError(f"Invalid storage connection string format: {e}") from e

    def get_queue_service_client(self) -> QueueServiceClient:
        """Returns an authenticated QueueServiceClient instance."""
        try:
            # Note: Functions often expect Base64 encoding for queue triggers/outputs.
            # Adjust policy if needed based on your specific binding configurations.
            # If using raw strings, set message_encode_policy=None, message_decode_policy=None.
            return QueueServiceClient.from_connection_string(
                conn_str=self.connection_string,
                message_encode_policy=TextBase64EncodePolicy(),  # Encodes outgoing messages to Base64
            )
        except ValueError as e:
            logging.error(
                msg=f"StorageService.get_queue_service_client: Invalid storage connection string format: {e}"
            )
            raise ValueError(f"Invalid storage connection string format: {e}") from e

    def get_table_service_client(self) -> TableServiceClient:
        """Returns an authenticated TableServiceClient instance."""
        try:
            return TableServiceClient.from_connection_string(
                conn_str=self.connection_string
            )
        except ValueError as e:
            logging.error(
                msg=f"StorageService.get_table_service_client: Invalid storage connection string format for Table "
                f"Service: {e}"
            )
            raise ValueError(
                f"Invalid storage connection string format for Table Service: {e}"
            ) from e

    def get_queue_client(self, queue_name: str) -> QueueClient:
        """
        Returns an authenticated QueueClient for a specific queue.
        Does NOT automatically create the queue.
        """
        if not queue_name:
            logging.error(
                msg="StorageService.get_queue_client: Queue name cannot be empty."
            )
            raise ValueError("Queue name cannot be empty.")
        try:
            # Inherits policy from service client if created that way, or specify explicitly
            return QueueClient.from_connection_string(
                self.connection_string,
                queue_name,
                message_encode_policy=TextBase64EncodePolicy(),
            )
        except ValueError as e:
            logging.error(
                msg=f"StorageService.get_queue_client: Invalid storage connection string or queue name "
                f"'{queue_name}': {e}"
            )
            raise ValueError(
                f"Invalid storage connection string or queue name '{queue_name}': {e}"
            ) from e

    def upload_blob_data(
        self,
        container_name: str,
        blob_name: str,
        data: Union[str, bytes, Dict, List],
        overwrite: bool = True,
    ):
        """
        Uploads data to Azure Blob Storage. Serializes Python dicts/lists to JSON.

        Args:
            container_name: The name of the blob container.
            blob_name: The name of the blob.
            data: The data to upload (str, bytes, dict, or list).
            overwrite: Whether to overwrite the blob if it already exists.

        Raises:
            ValueError: If container or blob name is invalid.
            TypeError: If data type is not supported.
            azure.core.exceptions.ServiceRequestError: For network issues.
            azure.core.exceptions.ResourceExistsError: If blob exists and overwrite is False.
        """
        if not container_name or not blob_name:
            logging.error(
                msg="StorageService.upload_blob_data: Container name and blob name cannot be empty."
            )
            raise ValueError("Container name and blob name cannot be empty.")

        logging.debug(
            msg=f"StorageService.upload_blob_data: Attempting to upload blob to {container_name}/{blob_name} "
            f"(overwrite={overwrite})"
        )
        try:
            blob_client: BlobClient = self.get_blob_service_client().get_blob_client(
                container=container_name, blob=blob_name
            )
        except ValueError as e:  # Catch connection string format error from getter
            logging.error(
                msg=f"StorageService.upload_blob_data: Failed to get blob client for {container_name}/{blob_name}: {e}"
            )
            raise

        try:
            upload_data: Union[str, bytes]
            settings_to_pass: Optional[ContentSettings] = None

            if isinstance(data, (dict, list)):
                upload_data = json.dumps(data, ensure_ascii=False).encode()
                settings_to_pass = ContentSettings(content_type="application/json")

            elif isinstance(data, str):
                upload_data = data.encode()
                settings_to_pass = ContentSettings(
                    content_type="text/plain; charset=utf-8"
                )

            elif isinstance(data, bytes):
                upload_data = data

            blob_client.upload_blob(
                data=upload_data,
                blob_type="BlockBlob",
                overwrite=overwrite,
                content_settings=settings_to_pass,
            )

            logging.info(
                msg=f"StorageService.upload_blob_data: Successfully uploaded blob: {container_name}/{blob_name}"
            )

        except ResourceExistsError as e:
            if not overwrite:
                logging.warning(
                    msg=f"StorageService.upload_blob_data: Blob {container_name}/{blob_name} already exists and "
                    f"overwrite is False."
                )
                raise
            else:
                logging.error(
                    msg=f"StorageService.upload_blob_data: Unexpected ResourceExistsError despite overwrite=True for "
                    f"{container_name}/{blob_name}: {e}"
                )
                raise
        except Exception as e:
            logging.error(
                msg=f"StorageService.upload_blob_data: Failed to upload blob {container_name}/{blob_name}: {e}"
            )
            raise

    def download_blob_as_text(
        self, container_name: str, blob_name: str, encoding: str = "utf-8"
    ) -> str:
        """
        Downloads blob content as a decoded text string.

        Args:
            container_name: The name of the blob container.
            blob_name: The name of the blob.
            encoding: The encoding to use for decoding the blob bytes.

        Returns:
            The decoded string content of the blob.

        Raises:
            ValueError: If container or blob name is invalid.
            azure.core.exceptions.ResourceNotFoundError: If the blob does not exist.
            azure.core.exceptions.ServiceRequestError: For network issues.
        """
        if not container_name or not blob_name:
            logging.error(
                msg="StorageService.download_blob_as_text: Container name and blob name cannot be empty."
            )
            raise ValueError("Container name and blob name cannot be empty.")

        logging.debug(
            msg=f"StorageService.download_blob_as_text: Attempting to download blob as text from "
            f"{container_name}/{blob_name}"
        )

        blob_client: BlobClient = self.get_blob_service_client().get_blob_client(
            container=container_name, blob=blob_name
        )

        try:
            blob_content_bytes: bytes = blob_client.download_blob().readall()
            logging.info(
                msg=f"StorageService.download_blob_as_text: Successfully downloaded blob: {container_name}/{blob_name}"
            )
            return blob_content_bytes.decode(encoding=encoding)
        except ResourceNotFoundError:
            logging.error(
                msg=f"StorageService.download_blob_as_text: Blob not found: {container_name}/{blob_name}"
            )
            raise
        except UnicodeDecodeError as e:
            logging.error(
                msg=f"StorageService.download_blob_as_text: Failed to decode blob {container_name}/{blob_name} "
                f"using encoding '{encoding}': {e}"
            )
            raise e
        except Exception as e:
            logging.error(
                msg=f"StorageService.download_blob_as_text: Failed to download blob {container_name}/{blob_name} "
                f"as text: {e}"
            )
            raise

    def download_blob_as_json(
        self, container_name: str, blob_name: str, encoding: str = "utf-8"
    ) -> Union[Dict, List]:
        """
        Downloads blob content and parses it as JSON.

        Args:
            container_name: The name of the blob container.
            blob_name: The name of the blob.
            encoding: The encoding of the stored text data before JSON parsing.

        Returns:
            A Python dictionary or list parsed from the blob's JSON content.

        Raises:
            ValueError: If container or blob name is invalid.
            json.JSONDecodeError: If the content cannot be parsed as JSON.
            azure.core.exceptions.ResourceNotFoundError: If the blob does not exist.
            azure.core.exceptions.ServiceRequestError: For network issues.
        """
        text_content: str = self.download_blob_as_text(
            container_name=container_name, blob_name=blob_name, encoding=encoding
        )

        try:
            return json.loads(text_content)
        except json.JSONDecodeError as e:
            logging.error(
                msg=f"StorageService.download_blob_as_json: Failed to parse blob content from "
                f"{container_name}/{blob_name} as JSON: {e}"
            )
            raise

    def list_blobs(
        self, container_name: str, name_starts_with: Optional[str] = None
    ) -> List[str]:
        """
        Lists blob names in a container, optionally filtering by prefix.

        Args:
            container_name: The name of the blob container.
            name_starts_with: Optional prefix to filter blob names.

        Returns:
            A list of blob names matching the criteria. Returns empty list if
            container not found.

        Raises:
            ValueError: If container name is invalid.
            azure.core.exceptions.ServiceRequestError: For network issues.
        """
        if not container_name:
            logging.error(
                msg="StorageService.list_blobs: Container name cannot be empty."
            )
            raise ValueError("Container name cannot be empty.")

        logging.debug(
            msg=f"StorageService.list_blobs: Listing blobs in container '{container_name}' starting with "
            f"'{name_starts_with or ''}'"
        )
        try:
            container_client: ContainerClient = (
                self.get_blob_service_client().get_container_client(
                    container=container_name
                )
            )
            blob_items: ItemPaged[BlobProperties] = container_client.list_blobs(
                name_starts_with=name_starts_with
            )
            blob_names: List[str] = [blob.name for blob in blob_items]
            logging.info(
                msg=f"StorageService.list_blobs: Found {len(blob_names)} blobs in '{container_name}' matching prefix "
                f"'{name_starts_with or ''}'."
            )
            return blob_names
        except ResourceNotFoundError:
            logging.warning(
                msg=f"StorageService.list_blobs: Container '{container_name}' not found while listing blobs."
            )
            return []
        except Exception as e:
            logging.error(
                msg=f"StorageService.list_blobs: Failed to list blobs in container '{container_name}': {e}"
            )
            raise

    def delete_blob(self, container_name: str, blob_name: str):
        """
        Deletes a specific blob. Does not raise error if blob doesn't exist.

        Args:
            container_name: The name of the blob container.
            blob_name: The name of the blob to delete.

        Raises:
            ValueError: If container or blob name is invalid.
            azure.core.exceptions.ServiceRequestError: For network issues.
        """
        if not container_name or not blob_name:
            logging.error(
                msg="StorageService.delete_blob: Container name and blob name cannot be empty."
            )
            raise ValueError("Container name and blob name cannot be empty.")

        logging.debug(
            msg=f"StorageService.delete_blob: Attempting to delete blob: {container_name}/{blob_name}"
        )
        blob_client: BlobClient = self.get_blob_service_client().get_blob_client(
            container=container_name, blob=blob_name
        )
        try:
            blob_client.delete_blob(delete_snapshots="include")
            logging.info(
                msg=f"StorageService.delete_blob: Successfully deleted blob: {container_name}/{blob_name}"
            )
        except ResourceNotFoundError:
            logging.warning(
                msg=f"StorageService.delete_blob: Blob not found during deletion, presumed already deleted: "
                f"{container_name}/{blob_name}"
            )
        except Exception as e:
            logging.error(
                msg=f"StorageService.delete_blob: Failed to delete blob {container_name}/{blob_name}: {e}"
            )
            raise

    def send_queue_message(
        self, queue_name: str, message_content: Union[Dict, List, str]
    ):
        """
        Sends a message to the specified Azure Queue. Serializes dicts/lists to JSON.

        Args:
            queue_name: The name of the target queue.
            message_content: The message content (dict, list, or string).

        Raises:
            ValueError: If queue name is invalid.
            TypeError: If message_content type is not supported.
            azure.core.exceptions.ServiceRequestError: For network issues.
        """
        if not queue_name:
            logging.error(
                msg="StorageService.send_queue_message: Queue name cannot be empty."
            )
            raise ValueError("Queue name cannot be empty.")

        message_str: str
        try:
            if isinstance(message_content, (dict, list)):
                message_str = json.dumps(message_content, ensure_ascii=False)
            elif isinstance(message_content, str):
                message_str = message_content

            logging.debug(
                msg=f"StorageService.send_queue_message: Attempting to send message to queue '{queue_name}': "
                f"{message_str[:100]}..."
            )
            queue_client: QueueClient = self.get_queue_client(queue_name=queue_name)
            queue_client.send_message(content=message_str)
            logging.info(
                msg=f"StorageService.send_queue_message: Successfully sent message to queue '{queue_name}'"
            )

        except Exception as e:
            logging.error(
                msg=f"StorageService.send_queue_message: Failed to send message to queue '{queue_name}': {e}"
            )
            raise

    def get_entities(
        self, table_name: str, filter_query: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieves entities from a specified Azure Table, with an optional filter.

        Args:
            table_name: The name of the table to query.
            filter_query: An OData filter string to apply to the query.
                          If None, all entities in the table are returned.
                          Example: "PartitionKey eq 'some_key'"

        Returns:
            A list of dictionaries, where each dictionary is an entity.
            Returns an empty list if the table does not exist.

        Raises:
            ValueError: If table_name is invalid.
            azure.core.exceptions.ServiceRequestError: For network or other service issues.
        """
        if not table_name:
            logging.error(
                msg="StorageService.get_entities: Table name cannot be empty."
            )
            raise ValueError("Table name cannot be empty.")

        logging.debug(
            msg=f"StorageService.get_entities: Querying entities from table '{table_name}' with filter: "
            f"'{filter_query or 'All'}'"
        )
        try:
            table_client: TableClient = (
                self.get_table_service_client().get_table_client(table_name=table_name)
            )
            entities: List[Dict[str, Any]]

            if filter_query:
                entities = list(table_client.query_entities(query_filter=filter_query))
            else:
                entities = list(table_client.list_entities())

            logging.info(
                msg=f"StorageService.get_entities: Retrieved {len(entities)} entities from table '{table_name}'."
            )
            return entities

        except ResourceNotFoundError:
            logging.warning(
                msg=f"StorageService.get_entities: Table '{table_name}' not found while querying entities. "
                f"Returning empty list."
            )
            return []
        except Exception as e:
            logging.error(
                msg=f"StorageService.get_entities: Failed to query entities from table '{table_name}': {e}",
                exc_info=True,
            )
            raise

    def delete_entity(self, table_name: str, partition_key: str, row_key: str) -> None:
        """
        Deletes a specific entity from an Azure Table.
        Does not raise an error if the entity does not exist.

        Args:
            table_name: The name of the target table.
            partition_key: The PartitionKey of the entity to delete.
            row_key: The RowKey of the entity to delete.

        Raises:
            ValueError: If any of the key arguments are invalid.
            azure.core.exceptions.ServiceRequestError: For network or other service issues.
        """
        if not all([table_name, partition_key, row_key]):
            logging.error(
                msg="StorageService.delete_entity: Table name, partition key, and row key cannot be empty."
            )
            raise ValueError("Table name, partition key, and row key cannot be empty.")

        logging.debug(
            msg=f"StorageService.delete_entity: Attempting to delete entity from {table_name} "
            f"with PK='{partition_key}' and RK='{row_key}'"
        )
        try:
            table_client: TableClient = (
                self.get_table_service_client().get_table_client(table_name=table_name)
            )
            table_client.delete_entity(partition_key=partition_key, row_key=row_key)
            logging.info(
                msg=f"StorageService.delete_entity: Successfully deleted entity from {table_name} with RowKey "
                f"'{row_key}'."
            )
        except ResourceNotFoundError:
            logging.warning(
                msg=f"StorageService.delete_entity: Entity not found during deletion, presumed already deleted: "
                f"Table='{table_name}', PK='{partition_key}', RK='{row_key}'"
            )
        except Exception as e:
            logging.error(
                msg=f"StorageService.delete_entity: Failed to delete entity from {table_name} with "
                f"RowKey '{row_key}': {e}",
                exc_info=True,
            )
            raise

    def upsert_entity(self, table_name: str, entity: Dict[str, Any]) -> None:
        """
        Inserts or updates an entity in the specified Azure Table.
        Creates the table if it does not exist.

        Args:
            table_name: The name of the target table.
            entity: A dictionary representing the entity to upsert.
                    Must contain 'PartitionKey' and 'RowKey'.

        Raises:
            ValueError: If table_name is invalid or entity is missing required keys.
            azure.core.exceptions.ServiceRequestError: For network or other service issues.
        """
        if not table_name:
            logging.error(
                msg="StorageService.upsert_entity: Table name cannot be empty."
            )
            raise ValueError("Table name cannot be empty.")
        if not all(k in entity for k in ["PartitionKey", "RowKey"]):
            logging.error(msg="Entity must contain 'PartitionKey' and 'RowKey'.")
            raise ValueError("Entity must contain 'PartitionKey' and 'RowKey'.")

        logging.debug(
            msg=f"StorageService.upsert_entity: Attempting to upsert entity into table '{table_name}'"
        )
        try:
            self.create_table_if_not_exists(table_name)
            table_client: TableClient = (
                self.get_table_service_client().get_table_client(table_name)
            )

            table_client.upsert_entity(entity=entity, mode=UpdateMode.REPLACE)
            logging.info(
                msg=f"StorageService.upsert_entity: Successfully upserted entity with RowKey '{entity.get('RowKey')}' "
                f"into table '{table_name}'."
            )

        except Exception as e:
            logging.error(
                msg=f"StorageService.upsert_entity: Failed to upsert entity into table '{table_name}': {e}",
                exc_info=True,
            )
            raise

    def create_table_if_not_exists(self, table_name: str):
        """
        Creates a table if it does not already exist.

        Args:
            table_name: The name of the table to create.
        """
        if not table_name:
            logging.error(
                "StorageService.create_table_if_not_exists: Table name cannot be empty."
            )
            raise ValueError("Table name cannot be empty.")

        try:
            table_service_client = self.get_table_service_client()
            table_service_client.create_table(table_name=table_name)
            logging.info(
                f"StorageService.create_table_if_not_exists: Table '{table_name}' created or already exists."
            )
        except ResourceExistsError:
            logging.debug(
                f"StorageService.create_table_if_not_exists: Table '{table_name}' already exists."
            )
        except Exception as e:
            logging.error(
                f"StorageService.create_table_if_not_exists: Failed to create table '{table_name}': {e}"
            )
            raise

    def delete_table(self, table_name: str):
        """
        Deletes a table. Does not raise an error if the table does not exist.

        Args:
            table_name: The name of the table to delete.
        """
        if not table_name:
            logging.error("StorageService.delete_table: Table name cannot be empty.")
            raise ValueError("Table name cannot be empty.")

        try:
            table_service_client = self.get_table_service_client()
            table_service_client.delete_table(table_name=table_name)
            logging.info(
                f"StorageService.delete_table: Successfully deleted table '{table_name}'."
            )
        except ResourceNotFoundError:
            logging.warning(
                f"StorageService.delete_table: Table '{table_name}' not found, presumed already deleted."
            )
        except Exception as e:
            logging.error(
                f"StorageService.delete_table: Failed to delete table '{table_name}': {e}"
            )
            raise

    def delete_entities_batch(
        self, table_name: str, entities: List[Dict[str, Any]]
    ) -> None:
        """
        Deletes a list of entities from a table in batches of 100.

        Args:
            table_name: The name of the target table.
            entities: A list of entity dictionaries to delete. Each must have PartitionKey and RowKey.
        """
        if not entities:
            return

        table_client = self.get_table_service_client().get_table_client(table_name)
        for i in range(0, len(entities), 100):
            batch = entities[i: i + 100]
            operations = [
                (
                    "delete",
                    TableEntity(PartitionKey=e["PartitionKey"], RowKey=e["RowKey"]),
                )
                for e in batch
            ]
            try:
                table_client.submit_transaction(operations=operations)
                logging.info(
                    f"Successfully deleted batch of {len(operations)} entities from '{table_name}'."
                )
            except Exception as e:
                logging.error(f"Error deleting batch from table '{table_name}': {e}")
                raise
