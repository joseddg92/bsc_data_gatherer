import logging
from queue import Queue
from threading import Thread, Lock
from typing import List, Any, Callable

from sqlalchemy import create_engine, text, desc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session, joinedload

from data_models import mapper_registry, Block, DexTradePair


class DDBBManager:
    logger = logging.getLogger(__name__)

    def __init__(self, ddbb_string: str, prune_schema=False):
        self.ddbb_string = ddbb_string
        self.__engine = self.__create_ddbb_engine(ddbb_string, prune_schema=prune_schema)
        self.__session: Session = sessionmaker(bind=self.__engine)()
        self.__work_queue = Queue()
        self.__session_lock = Lock()
        self.__worker_thread = Thread(name="DDBBManagerWorker", target=self.__worker_thread)

        if not self.__engine:
            raise ValueError("could not create DDBB engine")

        self.__worker_thread.start()


    def __session_work(self, work: Callable[[Session], Any], error_msg: str = ""):
        with self.__session_lock:
            try:
                return work(self.__session)
            except SQLAlchemyError:
                DDBBManager.logger.exception(f"{error_msg}")

    def __worker_thread(self):
        while True:
            entity = self.__work_queue.get()
            self.__session_work(lambda s: s.merge(entity), "Inserting entity")

    def __str__(self):
        return f"DDBBManager<{self.__engine}>"

    def get_last_block(self) -> Block:
        block_list = self.__session_work(
            lambda s: s \
                .query(Block) \
                .order_by(desc(Block.number)) \
                .limit(1).all()
        )


        return block_list[0] if block_list else None

    def get_all_pairs(self) -> List[DexTradePair]:
        return self.__session_work(
            lambda s: s \
                .query(DexTradePair) \
                .options(joinedload('*')) \
                .all(),
            "Getting all pairs"
        )

    def get_entity_by_pl(self, cls, primary_key_value):
        return self.__session_work(
            lambda s: s.query(cls).get(primary_key_value),
            "Getting entity by pk"
        )

    def commit_changes(self):
        def __work(session):
            if self.__work_queue.qsize() > 0:
                return False

            session.commit()
            return True

        while not self.__session_work(
            lambda s: __work(s),
            "Comiting changes"
        ):
            pass # Try again

    def persist(self, entity) -> None:
        self.__work_queue.put(entity)

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
