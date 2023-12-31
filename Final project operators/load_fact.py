from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend
class LoadFactOperator(BaseOperator):
    ui_color = '#358140'
    
    
    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                sql_statement="",
                *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.aws_credentials_id = aws_credentials_id
        self.execution_date = kwargs.get('execution_date')
    def execute(self, context):
      
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
       
        self.log.info(f'Inserting data into {self.table} fact table...')
       
        redshift.run(self.sql_statement)
       
        
        