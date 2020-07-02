from base.extractor import Extractor
from db.pg_accessor import PostgresAccessor
from util.cls_util import CodeUtil
from util.constants import ResourceConsts
from util.dict_util import DictUtil
from util.global_resources import GlobalResources
from util.sql_loader import SqlLoader


class PostgresStructureExtractor(Extractor):
    def __init__(self):
        self.res = GlobalResources()
        super().__init__("postgres-structure")

    def extract(self, **kwargs):
        db = PostgresAccessor(**DictUtil.filter(kwargs, CodeUtil.func_args(PostgresAccessor.__init__)))

        loader: SqlLoader
        loader, ok = self.res.get_safe(ResourceConsts.COMMANDS_LOADER)
        lt_q = loader.pick("ListTables")
        db.exec(lt_q)

        for v in db.fetch_all():
            print(v)



