from base.loader import Loader
from db.pg_accessor import PostgresAccessor
from extractors.postgres_structure import Table
from util.cls_util import CodeUtil
from util.constants import ResourceConsts
from util.dict_util import DictUtil
from util.global_resources import GlobalResources
from util.sql_loader import SqlLoader


class PostgresLoader(Loader):
    def __init__(self):
        super().__init__("postgres-loader")
        self.res = GlobalResources()

    def load(self, data=None, table: Table = None, time_machine=True, **kwargs):
        if table is None or data is None:
            raise RuntimeError("Must specify table and data!")
        db = PostgresAccessor.obtain(**DictUtil.filter(kwargs, CodeUtil.func_args(PostgresAccessor.__init__)))
        loader: SqlLoader
        loader, ok = self.res.get_safe(ResourceConsts.COMMANDS_LOADER)
        if not ok:
            raise RuntimeError("Internal Exception: builtin commands loader couldn't get accessed, make sure "
                               "data/commands.sql exists!")
        cols = [name for name, _, _, _ in table.columns]
        cols = ",".join(cols)
        cols_d = ["%s" for _ in table.columns]
        cols_d = ",".join(cols_d)
        for d in data:
            db.exec("""
                INSERT INTO %s (%s) VALUES (%s)
                """ % (table.name + ("_now" if time_machine else ""), cols, cols_d), d)
        db.commit()

    @staticmethod
    def name():
        return "postgres-loader"
