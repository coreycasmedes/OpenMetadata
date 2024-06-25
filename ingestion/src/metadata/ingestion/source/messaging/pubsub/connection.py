
from metadata.generated.schema.entity.services.connections.messaging.pubSubConnection import (
    PubSubConnection
)

from google.cloud import pubsub_v1



def get_connnection(connection: PubSubConnection) -> pubsub_v1.PublisherClient:
    publisher = pubsub_v1.PublisherClient()
    return publisher