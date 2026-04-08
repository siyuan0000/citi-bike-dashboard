from sqlalchemy import Column, Float, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Station(Base):
    __tablename__ = "station"

    station_id = Column(String, primary_key=True)
    name = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    capacity = Column(Integer)

    statuses = relationship("Status", back_populates="station")


class Status(Base):
    __tablename__ = "status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String, ForeignKey("station.station_id"), index=True)
    num_bikes_available = Column(Integer)
    num_docks_available = Column(Integer)
    grab_time = Column(Integer, index=True)
    is_installed = Column(Integer)
    is_renting = Column(Integer)
    is_returning = Column(Integer)
    num_bikes_disabled = Column(Integer)
    num_docks_disabled = Column(Integer)

    station = relationship("Station", back_populates="statuses")
