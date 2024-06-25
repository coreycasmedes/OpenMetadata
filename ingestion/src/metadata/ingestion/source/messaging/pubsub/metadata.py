
from typing import Iterable, List, Optional, Any
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.entity.services.connections.messaging.pubSubConnection import (
    PubSubConnection
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.source.messaging.messaging_service import MessagingServiceSource
from metadata.ingestion.api.models import Either
from metadata.generated.schema.api.data.createTopic import CreateTopicRequest

class PubSubSource(MessagingServiceSource):
    """
    PubSubSource class to fetch topics from serverless, non-broker based sources
    """
    def __init__(
        self, 
        config: WorkflowSource, 
        metadata: OpenMetadata
    ):
        super().__init__(config, metadata)
        self.pubsub = self.connection
        self.project_id = self.service_connection.project_id

    @classmethod
    def create(
        cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None
    ):
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection: PubSubConnection = config.serviceConnection.root.config
        if not isinstance(connection, PubSubConnection):
            raise InvalidSourceException(
                f"Expected PubSubConnection, but got {connection}"
            )
        return cls(config, metadata)



    def yield_topic(self, topic_details: Any) -> Iterable[Either[CreateTopicRequest]]:
        return super().yield_topic(topic_details)

    def get_topic_list(self) -> Optional[List[Any]]:
        return super().get_topic_list()

    def get_topic_name(self, topic_details: Any) -> str:
        return super().get_topic_name(topic_details)



