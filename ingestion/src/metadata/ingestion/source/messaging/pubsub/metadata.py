
from typing import Iterable, List, Optional, Any
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.entity.services.connections.messaging.pubSubConnection import (
    PubSubConnection
)
from metadata.ingestion.source.messaging.messaging_service import (
    BrokerTopicDetails,
    MessagingServiceSource,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.api.models import Either
from metadata.generated.schema.api.data.createTopic import CreateTopicRequest
from google.api_core.exceptions import GoogleAPIError
from metadata.ingestion.source.messaging.pubsub.models import (
    PubSubTopicMetadataModel,

)
from metadata.utils.logger import ingestion_logger
from google.pubsub_v1.types import Topic

logger = ingestion_logger()

class PubsubSource(MessagingServiceSource):
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
        self.project_id = self.service_connection.projectId

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

    def get_topic_list(self) -> Iterable[BrokerTopicDetails]:
        project_id = "openmetadata-integration-rk7e"
        #project_path = f"projects/{self.service_connection.projectId}"
        project_path = f"projects/{project_id}"
        project_topics = self.pubsub.list_topics(request={"project": project_path})
        try:
            for topic in project_topics:
                yield BrokerTopicDetails(
                    topic_name=topic.name,
                    topic_metadata=PubSubTopicMetadataModel(
                        kms_key_name=topic.kms_key_name,
                        labels=topic.labels,
                        message_retention_duration=topic.message_retention_duration,
                        schema_settings=topic.schema_settings,
                        


                    )
                )
        except GoogleAPIError as err:
            logger.error(f"An error occurred getting the PubSub topic list: {err}")
            




    def yield_topic(
        self, topic_details: BrokerTopicDetails
    ) -> Iterable[Either[CreateTopicRequest]]:
        pass

   

    def get_topic_name(self, topic_details: Any) -> str:
        return topic_details.name.split('/')[-1]



