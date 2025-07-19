from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import TEXT, INTEGER, BIGINT
from sqlalchemy.orm import Mapped, mapped_column

Base = declarative_base()

class Picture(Base):

    __tablename__ = "pulser_pictures"
    __table_args__ = {'schema': 'marketplaces'}

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(INTEGER)
    image_url: Mapped[str] = mapped_column(TEXT)
    path: Mapped[str] = mapped_column(TEXT)

    def __repr__(self):
        return f"Picture(picture_id={self.id!r}, " \
               f"source_id={self.source_id!r}, " \
               f"image_url={self.image_url!r}, " \
               f"path={self.path!r})"

    @staticmethod
    async def fill_picture(source_id : int, image_url : str, path : str):
        return Picture(source_id=source_id, image_url=image_url, path=path)