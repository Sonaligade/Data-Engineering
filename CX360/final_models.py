from pydantic import BaseModel
from typing import List, Optional, Union, Dict


class DataTranscript(BaseModel):
    full: Optional[dict[str, Optional[str]]] = {}         # Default to an empty dictionary if not provided
    agent: Optional[dict] = {}
    customer: Optional[dict] = {} 

class Sentiment(BaseModel):
    agent_sentiment: dict
    customer_sentiment: dict

class KeywordsTracking(BaseModel):
    agent_keywords_tracking: Optional[Dict] = {}
    customer_keywords_tracking: Optional[Dict] = {}

class CallData(BaseModel): 
    call_status: Optional[str]
    wait_time: Optional[str]
    hold_time: Optional[str] 
    transfered_call: Optional[str]
    language_detection: Optional[str] = ""    # Default empty string if not present
    transcript: DataTranscript 
    transcript_summary: Optional[str] = ""
    keywords_tracking: Optional[KeywordsTracking] = None
    Sentiment: Optional[Sentiment]
    agent_final_tone : Optional[str] = ""
    cust_final_tone : Optional[str] = ""
    customer_satisfaction_rates:Optional[str]
    customer_effort_score: Optional[str]
    NPS_Score: Optional[str]                  
    
class CallRecord(BaseModel):
    call_id: Optional[int]
    interaction_file_id:Optional[str]
    agent_id: Optional[int]
    client_id: Optional[int]
    customer_id : Optional[int]
    organisation_id: Optional[int]
    self_service:Optional[str]
    product_id: Optional[int]
    channel_id : Optional[str]
    department : Optional[str] 
    processes: Optional[str] = ""
    interaction_duration: Optional[str] = ""
    dead_air:Optional[str]  = ""
    call_data: CallData
    average_compliance_score:Optional[float] = 0.0  # Default 0.0 if not present
    ai_feedback: Optional[str] = ""
    ai_feedback_reason:Optional[str] = ""
    customer_intent: Optional[Dict] = {}   # Default empty dictionary if not present
    escalation:Optional[str]
    escalation_reason:Optional[str]
    manual_feedback: Optional[str]
    manual_rating: Optional[str]
    customer_rating: Optional[str]
    interaction_date:Optional[str] = ""

class Call(BaseModel):
    records: List[CallRecord]

    
# If you want to keep processes as a separate model
class UserRecord(BaseModel):
    user_id: Optional[int]  # Nullable field
    role_name: Optional[str]  # Nullable field
    first_name: Optional[str]  # Nullable field
    last_name: Optional[str]  # Nullable field
    email: Optional[str]  # Nullable field
    role_id: Optional[int]  # Nullable field
    qa_id: Optional[int]  # Nullable field
    manager_id: Optional[int]  # Nullable field
    is_active: Optional[bool]  # Nullable field
    date_joined: Optional[str]  # Nullable field, assuming string format
    user_email: Optional[str]  # Nullable field
    organisation_id: Optional[int]  # Nullable field
    customer_id: Optional[int]  # Nullable field
    processes: Optional[str]  # Nested model for processes (Optional)
    empcode: Optional[str]  # Nullable field
    user_mobile: Optional[str]  # Nullable field
    tenure: Optional[str]  # Nullable field
    bucket: Optional[str]  # Nullable field
    status: Optional[str]  # Nullable field

class User(BaseModel):
    records: List[UserRecord]  # List of user records


class CRM(BaseModel):
    customer_id : Optional[int]
    first_name : Optional[str]
    last_name : Optional[str]
    email : Optional[str]
    phone_number : Optional[str]
    DOB: Optional[str]
    gender: Optional[str]                  
    address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    postal_code: Optional[str]
    customer_income: Optional[float]
    preferred_language : Optional[str]
    total_spent : Optional[float]
    social_profile : Optional[str]
    customer_since : Optional[str]
    customer_status : Optional[str]
    customer_churn: Optional[str]

class CustomerCRM(BaseModel):
    records: List[CRM]


class Product_data(BaseModel):
    product_id : Optional[int]
    organisation_id : Optional[int]
    product_name : Optional[str]
    category : Optional[str]
    description : Optional[str]
    owner : Optional[str]
    price : Optional[str]
class Product(BaseModel):
    records: List[Product_data]

class Channel_data(BaseModel):
    channel_id : Optional[int]
    channel_name : Optional[str]
class Channel(BaseModel):
    records: List[Channel_data]

class OrganisationRecord(BaseModel):
    organisation_id:int 
    organisation_name: str
    organisation_email:str
    organisation_mobile: Optional[str]
    organisation_address: Optional[str]
    organisation_city: Optional[str]
    organisation_state: Optional[str]
    organisation_country: Optional[str]
    organisation_pincode: Optional[str]
    

class Organisation(BaseModel):
    records: List[OrganisationRecord]

class ClientRecord(BaseModel):
    client_id: int
    organisation_id: int
    client_first_name: Optional[str]
    client_last_name: Optional[str]
    client_language: Optional[str]
    client_email: Optional[str]
    client_mobile:Optional[str]


class Client(BaseModel):
    records: List[ClientRecord]




