from typing import Optional
from metadata.generated.schema.entity.services.connections.messaging.pubSubConnection import (
    PubSubConnection
)
from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.testConnectionResult import (
    TestConnectionResult,
)
from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.ometa.ometa_api import OpenMetadata

from google.cloud import pubsub_v1
from metadata.utils.constants import THREE_MIN

from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


def get_connection(connection: PubSubConnection) -> pubsub_v1.PublisherClient:
    publisher = pubsub_v1.PublisherClient()
    return publisher


def test_connection(
    metadata: OpenMetadata,
    client: pubsub_v1.PublisherClient,
    service_connection: PubSubConnection,
    automation_workflow: Optional[AutomationWorkflow] = None,
    timeout_seconds: Optional[int] = THREE_MIN,
) -> TestConnectionResult:
    """
    Test PubSub connection.
    """

    def list_topics_test():
        try:
            project_path = f"projects/{service_connection.projectId}"
            pubsub_topics = client.list_topics(request={"project": project_path})
            return list(pubsub_topics) is not None
        except Exception as err:
            logger.error(f"Error when testing PubSub connection: {err}")

    test_fn = {"GetTopics": list_topics_test}

    return test_connection_steps(
        metadata=metadata,
        service_type=service_connection.type.value,
        test_fn=test_fn,
        automation_workflow=automation_workflow,
        timeout_seconds=timeout_seconds
    )
    