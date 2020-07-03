from base.extractor import Extractor
from db.pg_accessor import PostgresAccessor
from util.cls_util import CodeUtil
from util.constants import ResourceConsts
from util.dict_util import DictUtil
from util.global_resources import GlobalResources
from util.sql_loader import SqlLoader


class PostgresExtractor(Extractor):
    def __init__(self):
        super().__init__("postgres-extractor")
        self.res = GlobalResources()

    def extract(self, offset=0, n=1, table=None, **kwargs):
        """
        Extracts n records with offset from some table
        :param offset: offset
        :param n: number of records
        :param table: source table
        :param kwargs: connection params
        :return:
        """
        if table is None:
            raise RuntimeError("Must specify table!")
        db = PostgresAccessor.obtain(**DictUtil.filter(kwargs, CodeUtil.func_args(PostgresAccessor.__init__)))
        loader: SqlLoader
        loader, ok = self.res.get_safe(ResourceConsts.COMMANDS_LOADER)
        if not ok:
            raise RuntimeError("Internal Exception: builtin commands loader couldn't get accessed, make sure "
                               "data/commands.sql exists!")
        gs_q = loader.pick("Generic Select", table=table, limit=n, offset=offset)
        db.exec(gs_q)
        return db.fetch_all()

    @staticmethod
    def name():
        return "postgres-extractor"
