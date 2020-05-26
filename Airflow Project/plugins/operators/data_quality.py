from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This operator serves the purpose of data quality check for user specified tables. Check whether the table is empty.
    
    Attributes:
        redshift_conn_id: Redshift connection ID to database. Please use Airflow connection settings to input this parameter.
        tables: a list of the names of tables for data quality check.
        
        *args, **kwargs: parameters from BaseOperator class.
    """
    
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 sql_query="",
                 result=0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables
        self.sql_query=sql_query
        self.result=result

    def execute(self, context):
        """
        the task execution function of DataQualityOperator class.
        Args: context: the context information of task instances of DataQualityOperator
        Return: None
        """
        
        
        self.log.info("Connecting to AWS Redshift...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("AWS Redshift Connected.")
        
        self.log.info("Start data quality check.")
        for table in self.tables:
            try:
                assert redshift.run(self.sql_query).format(table) == self.result
            except:
                self.log.info("Data quality check failed.")
        self.log.info("Data quality check passed.")