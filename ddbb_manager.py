import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List

from sqlalchemy import create_engine, text, desc
from sqlalchemy.orm import sessionmaker, Session, joinedload

from data_models import mapper_registry, Block, DexTradePair


class DDBBManager:
    logger = logging.getLogger(__name__)

    def __init__(self, ddbb_string: str, prune_schema=False):
        self.ddbb_string = ddbb_string
        self.__engine = self.__create_ddbb_engine(ddbb_string, prune_schema=prune_schema)
        self.__session: Session = sessionmaker(bind=self.__engine)()
        self.__executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='DDBBManagerWorker')

        if not self.__engine:
            raise ValueError("could not create DDBB engine")


    def __str__(self):
        return f"DDBBManager<{self.__engine}>"

    def get_last_block(self) -> Block:
        block_list = self.__executor.submit(
            lambda: self.__session \
                .query(Block) \
                .order_by(desc(Block.number)) \
                .limit(1).all()
        ).result()

        return block_list[0] if block_list else None

    def get_all_pairs(self) -> List[DexTradePair]:
        return self.__executor.submit(
            lambda: self.__session \
                .query(DexTradePair) \
                .options(joinedload('*')) \
                .all()
        ).result()

    def get_entity_by_pl(self, cls, primary_key_value):
        return self.__executor.submit(
            lambda: self.__session.query(cls).get(primary_key_value)
        ).result()

    def commit_changes(self, sync=False):
        f = self.__executor.submit(
            lambda: self.__session.commit()
        )

        if sync:
            f.result()


    def persist(self, entity, sync=False) -> None:
        f = self.__executor.submit(
            lambda: self.__session.merge(entity)
        )

        if sync:
            f.result()

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
