from typing import List, Tuple, Dict

from base.loader import Loader
from base.transformer import Transformer
from db.pg_accessor import PostgresAccessor
from extractors.postgres_structure import Table
from util.cls_util import CodeUtil
from util.constants import ResourceConsts
from util.dict_util import DictUtil
from util.global_resources import GlobalResources
from util.sql_loader import SqlLoader


class PostgresStructureLoader(Loader):
    """
    Extracts and returns structures of all Tables in a Database
    """

    def __init__(self):
        self.res = GlobalResources()
        super().__init__("postgres-structure")

    def load(self, time_machine=True, structure: Tuple[Dict[str, Table], List[str]] = None, **kwargs):
        """
        Extracts and returns structures of all Tables in a Database + DAG Of Tables
        :param time_machine: Whether use time_machine or not
        :param structure: Structure of Tables + DAG
        :param kwargs: keyword arguments used for extraction options as:
             - host: str = "localhost"
             - port: int = 5432
             - db: str = "postgres"
             - user: str = ""
             - pwd: str = ""
        :return: Dict[str, Table]
        """
        if structure is None:
            raise RuntimeError("Structure must've been provided!")

        db = PostgresAccessor.obtain(**DictUtil.filter(kwargs, CodeUtil.func_args(PostgresAccessor.__init__)))

        loader: SqlLoader
        loader, ok = self.res.get_safe(ResourceConsts.COMMANDS_LOADER)
        if not ok:
            raise RuntimeError("Internal Exception: builtin commands loader couldn't get accessed, make sure "
                               "data/commands.sql exists!")
        # -- Reading Structures
        tables, top_order = structure
        if time_machine:
            db.exec(loader.pick("Gist Extension"))
            db.exec(loader.pick("Set TimeMachine Time"))
        total_query = []
        for t in top_order:
            if time_machine:
                total_query.append(TimeMachineTable(tables[t]).to_sql(loader) + "\n")
            else:
                total_query.append(tables[t].to_sql() + "\n")
        for query in total_query:
            # try:
            db.exec(query)
            db.commit()
            # except:
            #     print(query)
            #     break
        pass

    @staticmethod
    def name():
        return "postgres-structure"


class TimeMachineTable:
    def __init__(self, table: Table):
        self.table = table

    def to_sql(self, loader: SqlLoader):
        # Create Main Table
        q = "CREATE TABLE %s(\n" % self.table.name
        cmds = []
        for name, datatype, nullable, max_len_char in self.table.columns:
            c = "%s %s%s%s" % (name, datatype,
                               "(%d)" % max_len_char if max_len_char is not None else "",
                               " NOT NULL" if not nullable else "")
            cmds.append(c)
        cmds.append("__lifetime tstzrange")
        mini_c = []
        for k in self.table.keys:
            mini_c.append("%s WITH =" % k)
        mini_c.append("__lifetime WITH &&")
        # cmds.append("PRIMARY KEY (%s)" % (",".join(self.table.keys + ["__lifetime"])))
        cmds.append("EXCLUDE USING gist (%s)" % (",".join(mini_c)))
        # cmds.append("PRIMARY KEY(%s)" % ",".join(self.table.keys))
        # for name, column, target_table, target_column in self.table.fk_relations:
            # c = "CONSTRAINT %s (%s) \n\t\t REFERENCES %s(%s)"
            # c = "CONSTRAINT %s \n\t FOREIGN KEY (%s, __lifetime) \n\t\t REFERENCES %s(%s, __lifetime)"
            # c = c % (name, column, target_table, target_column)
            # cmds.append(c)
        q += ",\n".join(cmds)
        q += "\n);"
        # Create TimeMachine Procedure Function For Trigger
        cols = [name for name, _, _, _ in self.table.columns]
        cols = list(filter(lambda x: x not in self.table.keys, cols))

        key_chk = ["NEW.%s <> OLD.%s" % (k, k) for k in self.table.keys]
        key_chk = " OR ".join(key_chk)

        id_search = ["%s = NEW.%s" % (k, k) for k in self.table.keys]
        id_search = " AND ".join(id_search)

        id_search_old = ["%s = OLD.%s" % (k, k) for k in self.table.keys]
        id_search_old = " AND ".join(id_search_old)

        id_cols_new = ["NEW.%s" % k for k in self.table.keys]
        id_cols_new = ",".join(id_cols_new)

        id_cols = [k for k in self.table.keys]
        id_cols = ",".join(id_cols)

        cols_n = ["NEW.%s" % c for c in cols]
        cols_s = "," + ",".join(cols) if len(cols) != 0 else ""
        cols_n = "," + ",".join(cols_n) if len(cols_n) != 0 else ""
        q += "\n" + loader.pick("TimeMachine Func", table=self.table.name,
                                confliction_check=key_chk,
                                id_search=id_search,
                                id_cols=id_cols,
                                id_cols_new=id_cols_new,
                                id_search_old=id_search_old,
                                cols=cols_s, col_datas=cols_n)
        # Create TimeMachine View:
        q += "\n" + loader.pick("TimeMachine View", table=self.table.name,
                                cols=",".join(self.table.keys + cols))
        # Create TimeMachine View Now:
        q += "\n" + loader.pick("TimeMachine View Now", table=self.table.name,
                                cols=",".join(self.table.keys + cols))
        # Create Time Machine Trigger:
        q += "\n" + loader.pick("TimeMachine Trigger", table=self.table.name)
        return q
