import uuid
from datetime import datetime

from sqlalchemy import TIMESTAMP, TEXT
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped, mapped_column

Base = declarative_base()


class Source(Base):
    __tablename__ = "sources"
    __table_args__ = {'schema': 'marketplaces'}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(TEXT, unique=True)
    relevance: Mapped[datetime] = mapped_column(TIMESTAMP)
