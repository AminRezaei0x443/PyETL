from pipeline.pg2pg import Pg2PgPipeline
from util.constants import ResourceConsts
from util.global_resources import GlobalResources
from util.sql_loader import SqlLoader

if __name__ == "__main__":
    resources = GlobalResources()
    sqlLoader = SqlLoader("data/commands.sql")
    resources.add(ResourceConsts.COMMANDS_LOADER, sqlLoader)

    pipeline = Pg2PgPipeline()
    # connection is connection params:
    '''
         - host: str = "localhost"
         - port: int = 5432
         - db: str = "postgres"
         - user: str = ""
         - pwd: str = ""
    '''
    pipeline.run(constructing=True, src_connection={

    }, target_connection={
        "db": "library"
    })
