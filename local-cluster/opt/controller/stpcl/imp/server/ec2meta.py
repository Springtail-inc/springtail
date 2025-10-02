# Description: Simulating the EC2 metadata service for local testing
from fastapi import APIRouter

router = APIRouter()


@router.put("/api/token")
async def put_token():
    return "test-token"


@router.get("/meta-data/instance-id")
async def get_instance_id():
    return "i-1234567890abcdef0"
