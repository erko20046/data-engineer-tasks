from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import TEXT, SMALLINT, INTEGER
from sqlalchemy.orm import Mapped, mapped_column

Base = declarative_base()


class Category(Base):
    __tablename__ = "upack_categories"
    __table_args__ = {'schema': 'marketplaces'}

    category_id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    parent_id: Mapped[int] = mapped_column(INTEGER, nullable=True)
    name: Mapped[str] = mapped_column(TEXT)
    category_url: Mapped[str] = mapped_column(TEXT)
    priority: Mapped[int] = mapped_column(SMALLINT)

