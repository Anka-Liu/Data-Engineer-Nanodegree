from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    This operator serves the purpose of copying data from s3 bucket to staging tables.
    
    Attributes:
        redshift_conn_id: Redshift connection ID to database. Please use Airflow connection settings to input this parameter.
        aws_credentials: AWS credential keys. Please use Airflow connection settings to input this parameter.
        table: the name of stage table.
        s3_bucket: the path of s3 bucket.
        s3_key: the target folder that stores json files.
        json_option: option for copying json files. 'auto' makes copy statement automatically guess json file structures, 
                     or use the jsonpath file to indicate json file structure.
        *args, **kwargs: parameters from BaseOperator class.
    """
    
    
    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_option="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials = aws_credentials
        self.json_option = json_option

    def execute(self, context):
        """
        the task execution function of StageToRedshiftOperator class.
        Args: context: the context information of task instances of StageToRedshiftOperator
        Return: None
        """
        
        
        self.log.info("Connecting to AWS Redshift...")
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("AWS Redshift connected.")


        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Data Cleared.")
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key=self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_option
        )
        redshift.run(formatted_sql)
        self.log.info("Data successfully copied from S3 Bucket.")




