from metadata.ingestion.source.messaging.pubsub.metadata import PubsubSource
from metadata.utils.service_spec import BaseSpec

ServiceSpec = BaseSpec(metadata_source_class=PubsubSource)
