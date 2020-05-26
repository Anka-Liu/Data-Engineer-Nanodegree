from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    This operator serves the purpose of inserting fact tables with data from stage tables.
    
    Attributes:
        redshift_conn_id: Redshift connection ID to database. Please use Airflow connection settings to input this parameter.
        fact_table: the name of fact table.
        sql_query: the sql query that conducts inserting fact tables with data from stage tables.
        *args, **kwargs: parameters from BaseOperator class.
    """
    
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.fact_table=fact_table
        self.sql_query=sql_query


    def execute(self, context):
        """
        the task execution function of LoadFactOperator class.
        Args: context: the context information of task instances of LoadFactOperator
        Return: None
        """
        
        
        self.log.info("Connecting to AWS Redshift...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("AWS Redshift Connected.")
        
        sql_query='insert into {} {}'.format(self.fact_table,self.sql_query)
        
        redshift.run(sql_query)
        self.log.info("Fact table successfully inserted.")
        
