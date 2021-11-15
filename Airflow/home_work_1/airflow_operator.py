import base64
import datetime as dt
from typing import Dict, Union
from google.cloud import storage as st
import logging as log
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail,Attachment,FileContent,FileName,FileType,Disposition
from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


LOG = log.getLogger(__name__)
ST_HOOK = GoogleCloudStorageHook(google_cloud_storage_conn_id='gcp_connection')
ST_CLIENT = st.Client(project = ST_HOOK._get_field('project'), credentials = ST_HOOK._get_credentials())
ST_BUCKET_ID = Variable.get('ST_BUCKET_ID')
SENDGRID_AK = Variable.get('SENDGRID_API_KEY')
EMAIL = Variable.get('EMAIL')
CURR_DATE = str(dt.datetime.now().date())


class SendEmailOperator(BaseOperator):
    """Class for creating custom Airflow python operator for
    sending email with csv report."""

    @apply_defaults
    def __init__(self, my_operator_param, *args, **kwargs):
        self.operator_param = my_operator_param
        super(SendEmailOperator, self).__init__(*args, **kwargs)

    def download_csv(self, bucket_id: str) -> str:
        """Method for downloading csv file from GCP storage."""
        bucket = ST_CLIENT.get_bucket(bucket_id)
        blob = bucket.blob(f'report_{CURR_DATE}.csv')
        return blob.download_as_string()

    def send_email(self, recipient: str, csv: str) -> Dict[str, Union[int, str]]:
        """Method for sending email with csv file."""
        message = Mail(from_email='maximzltrv@gmail.com',
                       to_emails=recipient,
                       subject=f'My first email from Airflow {CURR_DATE}',
                       html_content='<strong>Here is your first .csv report from Airflow.')
        encoded_file = base64.b64encode(csv).decode()
        attachedFile = Attachment(FileContent(encoded_file),
                                  FileName(f'report_{CURR_DATE}.csv'),
                                  FileType('application/csv'),
                                  Disposition('attachment'))
        message.attachment = attachedFile
        sg = SendGridAPIClient(SENDGRID_AK)
        response = sg.send(message)
        return {'status_code': response.status_code, 'body': response.body, 'headers': response.headers}

    def execute(self, context):
        """Main Airflow operator method."""
        LOG.info('START WORK.')

        LOG.info('Start downloading .csv file from GoogleCloudStorage.')
        csv_data = self.download_csv(ST_BUCKET_ID)
        LOG.info('Step completed succesful.')
        LOG.info(f'Start sending email to {EMAIL}.')
        response = self.send_email(EMAIL, csv_data)
        if response['status_code'] == 202:
            LOG.info('Step completed succesful.')
        else:
            LOG.info('Step failed.')

        LOG.info('FINISH WORK.')


class SendEmailPlugin(AirflowPlugin):
    """Class for creating custom DAG plugin
    for downloading csv report and sending email."""
    name = 'download-csv-send-report'
    operators = [SendEmailOperator]
