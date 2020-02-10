from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",                 
                 table="",
                 sql="",
                 overwrite=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id                 
        self.table=table
        self.sql=sql
        self.overwrite = overwrite

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.overwrite:
            self.log.info(f"Truncating {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        self.log.info(f"Pouplating data in to {self.table}")
        redshift.run(self.sql)