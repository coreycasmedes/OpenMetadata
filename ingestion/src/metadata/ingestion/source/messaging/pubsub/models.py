from enum import Enum
from typing import List, Optional, Dict
from datetime import timedelta

from pydantic import BaseModel, Extra

class PubSubTopicMetadataModel(BaseModel):
    """
    Model for PubSub Topic Metadata
    """
    message_retention_duration: Optional[timedelta] = None
    kms_key_name: Optional[str] = None
    message_storage_policy: Optional[Dict[str, Optional[bool]]] = None
    schema_settings: Optional[Dict[str, str]] = None

    # m: Optional[KinesisSummaryModel]
    # partitions: Optional[List[str]]
