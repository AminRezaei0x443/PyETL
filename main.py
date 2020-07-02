from base.extract import Extract
from extractors.postgres_structure import PostgresStructureExtractor
from util.constants import ResourceConsts
from db.db_accessor import DatabaseAccessor
from util.global_resources import GlobalResources
from util.sql_loader import SqlLoader

if __name__ == "__main__":
    resources = GlobalResources()
    sqlLoader = SqlLoader("data/commands.sql")
    resources.add(ResourceConsts.COMMANDS_LOADER, sqlLoader)
    source = DatabaseAccessor()
    extract = Extract()
    extract.define(PostgresStructureExtractor())
    extract.add("postgres-structure")
    extract()
