from base.ETLPipeline import ETLPipeline
from db.pg_accessor import PostgresAccessor
from extractors.postgres import PostgresExtractor
from extractors.postgres_structure import PostgresStructureExtractor
from fixer.pg_consistency import PostgresConsistencyFixer
from loaders.postgres import PostgresLoader
from loaders.postgres_structure import PostgresStructureLoader


class Pg2PgPipeline(ETLPipeline):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.def_extractor(PostgresExtractor())
        self.def_loader(PostgresLoader())
        self.def_structure_extractor(PostgresStructureExtractor())
        self.def_structure_loader(PostgresStructureLoader())

    def run(self, constructing=True, src_connection=None, target_connection=None,
            time_machine=True, **kwargs):
        if target_connection is None:
            target_connection = {}
        if src_connection is None:
            src_connection = {}
        self.add_structure_extractor(PostgresStructureExtractor.name(), **src_connection)
        extract_result = self.structure_extract()
        tables, top_order = extract_result[PostgresStructureExtractor.name()]
        if constructing:
            self.add_structure_loader(PostgresStructureLoader.name(),
                                      time_machine=time_machine,
                                      structure=(tables, top_order),
                                      **target_connection)
            self.structure_load()

            for t in top_order:
                table = tables[t]
                for i in range(table.count):
                    self.add_extractor(PostgresExtractor.name(), offset=i, table=t, **src_connection)
                    d = self.extract()[PostgresExtractor.name()]
                    self.add_loader(PostgresLoader.name(), data=d, table=table, **target_connection)
                    self.load()

        fixer = PostgresConsistencyFixer()
        fixer.fix(PostgresAccessor.obtain(**src_connection),
                  PostgresAccessor.obtain(**target_connection),
                  structure=(tables, top_order),
                  time_machine=time_machine)
