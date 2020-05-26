from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This operator serves the purpose of inserting dimension table with data from stage tables.
    
    Attributes:
        redshift_conn_id: Redshift connection ID to database. Please use Airflow connection settings to input this parameter.
        fact_table: the name of dimension table.
        sql_query: the sql query that conducts inserting dimension table with data from stage tables.
        *args, **kwargs: parameters from BaseOperator class.
    """
    
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dimension_table="",
                 sql_query="",
                 mode="overwrite",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dimension_table=dimension_table
        self.sql_query=sql_query
        self.mode=mode

    def execute(self, context):
        """
        the task execution function of LoadDimensionOperator class.
        Args: context: the context information of task instances of LoadDimensionOperator
        Return: None
        """
        
        
        self.log.info("Connecting to AWS Redshift...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("AWS Redshift Connected.")
        
        
        if self.mode == "overwrite":
            self.log.info("Mode: overwrite")
            sql_query="""
            truncate {};
            insert into {} {}
            """.format(self.dimension_table,self.dimension_table,self.sql_query)
        elif self.mode == "insert":
            self.log.info("Mode: insert")
            sql_query='insert into {} {}'.format(self.dimension_table,self.sql_query)
        redshift.run(sql_query)
        self.log.info("Dimension table successfully inserted.")
        