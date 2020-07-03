from typing import Dict, List, Tuple

from db.pg_accessor import PostgresAccessor
from extractors.postgres_structure import Table
from fixer.fixer import Fixer
from util.constants import ResourceConsts
from util.global_resources import GlobalResources
from util.sql_loader import SqlLoader


class PostgresConsistencyFixer(Fixer):
    def __init__(self):
        self.res = GlobalResources()
        super().__init__("pg-consistency")

    def fix(self, db_src: PostgresAccessor, db_target: PostgresAccessor,
            structure: Tuple[Dict[str, Table], List[str]] = None,
            time_machine=True, **kwargs):
        """
        Fixes and provides consistency checks
        :param db_src: source db
        :param db_target: target db
        :param structure: structures of source db
        :param time_machine: whether to use time machine
        :param kwargs: ...
        :return:
        """
        if structure is None:
            raise RuntimeError("Structure must've been provided!")
        tables, top_order = structure

        loader: SqlLoader
        loader, ok = self.res.get_safe(ResourceConsts.COMMANDS_LOADER)
        # Check Insertions
        for t in top_order:
            cols = ",".join(tables[t].keys)
            source_q = loader.pick("ID Cols All", id=cols, table=t)
            target_q = loader.pick("ID Cols All", id=cols, table=t+("_now" if time_machine else ""))
            db_src.exec(source_q)
            source = set(list(db_src.fetch_all()))
            db_target.exec(target_q)
            target = set(list(db_target.fetch_all()))

            insertion_ids = source.difference(target)
            for _id in insertion_ids:
                conds = ["%s = %%s" % k for k in tables[t].keys]
                q = loader.pick("Generic Select Cond", table=t, conditions=" AND ".join(conds))
                db_src.exec(q, _id)
                d = db_src.fetch_one()

                table = tables[t]
                cols = [name for name, _, _, _ in table.columns]
                cols = ",".join(cols)
                cols_d = ["%s" for _ in table.columns]
                cols_d = ",".join(cols_d)
                db_target.exec("""
                    INSERT INTO %s (%s) VALUES (%s)
                    """ % (table.name + ("_now" if time_machine else ""), cols, cols_d), d)
        # Check Removals
        for t in reversed(top_order):
            cols = ",".join(tables[t].keys)
            source_q = loader.pick("ID Cols All", id=cols, table=t)
            target_q = loader.pick("ID Cols All", id=cols, table=t+("_now" if time_machine else ""))
            db_src.exec(source_q)
            source = set(list(db_src.fetch_all()))
            db_target.exec(target_q)
            target = set(list(db_target.fetch_all()))

            deleted_ids = target.difference(source)
            for _id in deleted_ids:
                conds = ["%s = %%s" % k for k in tables[t].keys]
                q = loader.pick("Generic Delete Cond", table=t, conditions=" AND ".join(conds))
                db_target.exec(q, _id)
        # Check Updates
        for t in reversed(top_order):
            cols = ",".join(tables[t].keys)
            source_q = loader.pick("ID Cols All", id=cols, table=t)
            db_src.exec(source_q)
            source = set(list(db_src.fetch_all()))

            for _id in source:
                conds = ["%s = %%s" % k for k in tables[t].keys]
                s_q = loader.pick("Generic Select Cond", table=t, conditions=" AND ".join(conds))
                t_q = loader.pick("Generic Select Cond", table=t+("_now" if time_machine else ""),
                                  conditions=" AND ".join(conds))
                db_src.exec(s_q, _id)
                d_s = db_src.fetch_one()
                db_target.exec(t_q, _id)
                d_t = db_target.fetch_one()

                if d_s == d_t:
                    continue

                u_conds = ["%s = %%s" % k for k in tables[t].columns]
                u_q = loader.pick("Generic Update Cond", table=t,
                                  update_conditions=" AND ".join(u_conds),
                                  conditions=" AND ".join(conds))

                db_target.exec(u_q, d_t + _id)
