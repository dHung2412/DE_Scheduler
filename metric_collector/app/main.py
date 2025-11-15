from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager
import asyncio

from . import kafka_producer
from .shared_queue import metric_queue