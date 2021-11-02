import logging

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session

from data_models import mapper_registry


class DDBBManager:
    logger = logging.getLogger(__name__)

    def __init__(self, ddbb_string: str, prune_schema=False):
        self.ddbb_string = ddbb_string
        self.__engine = self.__create_ddbb_engine(ddbb_string, prune_schema=prune_schema)
        self.__session: Session = sessionmaker(bind=self.__engine)()

        if not self.__engine:
            raise ValueError("could not create DDBB engine")

    def __str__(self):
        return f"DDBBManager<{self.__engine}>"

    def persist(self, entity, commit=True) -> None:
        try:
            if isinstance(entity, list):
                for e in entity:
                    self.__session.merge(e)
            else:
                self.__session.merge(entity)
            if commit:
                self.__session.commit()
        except SQLAlchemyError:
            DDBBManager.logger.exception(f"Inserting {entity}")
            self.__session.rollback()

    @staticmethod
    def __create_ddbb_engine(ddbb_string, prune_schema=False):
        engine = None
        if ddbb_string:
            try:
                engine = create_engine(ddbb_string)
                with engine.connect() as conn:
                    # Check connection just in case
                    conn.execute(text("SELECT 1"))
                    if prune_schema:
                        conn.execute(text("drop schema public CASCADE; create schema public"))

                mapper_registry.metadata.create_all(engine)
            except (Exception,):
                engine = None
                DDBBManager.logger.exception("could not create ddbb engine")

        return engine
