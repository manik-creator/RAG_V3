from pydantic import BaseModel, Field
from typing import Optional

class UnifiedAssetSchema(BaseModel):
    ID: str = Field(description="Unique identifier for the asset")
    Name: Optional[str] = Field(description="Display name of the asset")
    Host_name: Optional[str] = Field(description="Host name or DNS name")
    IP_Address: Optional[str] = Field(description="Primary IP address")
    Memory: Optional[str] = Field(description="Memory capacity or Type")
    Cloud_Provider: str = Field(description="Cloud provider (aws, azure, gcp, etc.)")
    Cloud_Meta_Instance_ID: Optional[str] = Field(description="Provider-specific instance ID or availability zone")
    Cloud_Account_ID: Optional[str] = Field(description="Identifier for the cloud account/subscription")
