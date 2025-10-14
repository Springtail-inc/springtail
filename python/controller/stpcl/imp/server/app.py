# Description: FastAPI (https://fastapi.tiangolo.com/) application with EC2 metadata and SNS routes
# This file sets up a FastAPI application with a lifespan context manager to handle per-process resources.
# - EC2 metadata routes are included under the "/latest" prefix. So the clients can query that path for metadata just
#   like they would on an actual EC2 instance.
# - SNS routes are included under the "/sns" prefix to handle SNS-related functionality. This is the HTTP endpoint for
#   receiving SNS notifications (have not been hooked up to actual SNS topics yet, will do in the next iteration).
# - TODO: The server will also surface the Service status, Redis System Settings etc in the future.
from fastapi import (
    FastAPI,
)
from contextlib import asynccontextmanager
import boto3
from . import (
    ec2meta,
    sns,
)


class Registry:
    def __init__(self):
        session = boto3.session.Session()
        self.sns_client = session.client('sns')


@asynccontextmanager
async def lifespan(a: FastAPI):
    # Create resources per worker process. (Process-local globals)
    a.state.registry = Registry()
    try:
        yield
    finally:
        # Clean up per process here, if needed
        pass


app = FastAPI(lifespan=lifespan)

# Include routers to server HTTP traffics
app.include_router(ec2meta.router, prefix="/latest")
app.include_router(sns.router, prefix="/sns")
