import json

import networkx as nx

from base.extractor import Extractor
from db.pg_accessor import PostgresAccessor
from util.cls_util import CodeUtil
from util.constants import ResourceConsts
from util.dict_util import DictUtil
from util.global_resources import GlobalResources
from util.sql_loader import SqlLoader


class PostgresStructureExtractor(Extractor):
    """
    Extracts and returns structures of all Tables in a Database
    """
    def __init__(self):
        self.res = GlobalResources()
        super().__init__("postgres-structure")

    def extract(self, **kwargs):
        """
        Extracts and returns structures of all Tables in a Database + DAG Of Tables
        :param kwargs: keyword arguments used for extraction options as:
             - host: str = "localhost"
             - port: int = 5432
             - db: str = "postgres"
             - user: str = ""
             - pwd: str = ""
        :return: Dict[str, Table]
        """
        db = PostgresAccessor(**DictUtil.filter(kwargs, CodeUtil.func_args(PostgresAccessor.__init__)))

        result = {}

        loader: SqlLoader
        loader, ok = self.res.get_safe(ResourceConsts.COMMANDS_LOADER)
        if not ok:
            raise RuntimeError("Internal Exception: builtin commands loader couldn't get accessed, make sure "
                               "data/commands.sql exists!")
        # -- Extracting Structures
        lt_q = loader.pick("ListTables")
        db.exec(lt_q)
        table_names = [v[0] for v in db.fetch_all()]

        for table in table_names:
            t = Table(table)
            result[table] = t
            tc_q = loader.pick("TableColumns", table=table)
            db.exec(tc_q)
            for c in db.fetch_all():
                t.add_column(c[0], c[1], c[2].lower() != "NO".lower(), c[3])
            tk_q = loader.pick("TableKeys", table=table)
            db.exec(tk_q)
            for v in db.fetch_all():
                t.add_key(v[0])
            fkr_q = loader.pick("ForeignKeyRelations", table=table)
            db.exec(fkr_q)
            for r in db.fetch_all():
                t.add_relation(r[0], r[1], r[2], r[3])
        # -- Extracting DAG
        ed = []

        dag_q = loader.pick("AllTablesForeignKeyRelations")
        db.exec(dag_q)

        for r in db.fetch_all():
            ed.append((r[1], r[0]))
        g = nx.DiGraph(ed)
        return result, list(nx.topological_sort(g))


class Table:
    def __init__(self, name):
        self.name = name
        self.columns = []
        self.keys = []
        self.fk_relations = []

    def add_column(self, name: str, datatype: str, nullable: bool, max_len_char: int):
        self.columns.append((name, datatype, nullable, max_len_char))

    def add_key(self, key):
        self.keys.append(key)

    def add_relation(self, name, column, target_table, target_column):
        self.fk_relations.append((name, column, target_table, target_column))

    def to_sql(self):
        q = "CREATE TABLE %s(\n" % self.name
        cmds = []
        for name, datatype, nullable, max_len_char in self.columns:
            c = "%s %s%s%s" % (name, datatype,
                               "(%d)" % max_len_char if max_len_char is not None else "",
                               " NOT NULL" if not nullable else "")
            cmds.append(c)
        cmds.append("PRIMARY KEY(%s)" % ",".join(self.keys))
        for name, column, target_table, target_column in self.fk_relations:
            c = "CONSTRAINT %s \n\t FOREIGN KEY (%s) \n\t\t REFERENCES %s(%s)"
            c = c % (name, column, target_table, target_column)
            cmds.append(c)
        q += ",\n".join(cmds)
        q += "\n);"
        return q

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return json.dumps({
            "name": self.name,
            "columns": self.columns,
            "keys": self.keys,
            "relations": self.fk_relations
        })
