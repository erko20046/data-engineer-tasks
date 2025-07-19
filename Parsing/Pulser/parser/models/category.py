from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import TEXT, SMALLINT, INTEGER
from sqlalchemy.orm import Mapped, mapped_column

Base = declarative_base()

class Category(Base):

    __tablename__ = "pulser_categories"
    __table_args__ = {'schema': 'marketplaces'}

    category_id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    parent_id: Mapped[int] = mapped_column(INTEGER, nullable=True)
    name: Mapped[str] = mapped_column(TEXT)
    priority: Mapped[int] = mapped_column(SMALLINT)

    def __repr__(self):
        return f"Category(category_id={self.category_id!r}, " \
               f"parent_id={self.parent_id!r}, " \
               f"name={self.name!r}, "\
               f"priority={self.priority!r})"

    @staticmethod
    async def fill_category(category_id:int, parent_id: [int | None], name: str, priority: int):
        return Category(
                    category_id=category_id,
                    parent_id=parent_id,
                    name=name,
                    priority=priority
                )