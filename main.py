# install mopinion library
#pip install mopinion
#pip install fastparquet
#pip install google-cloud-bigquery

# use mopinion, GCP SDK and pandas
from mopinion import MopinionClient
from google.cloud import storage
from google.cloud import bigquery
bq_client = bigquery.Client.from_service_account_json(
        '../ods-nl-dnoi-creds.json')
import pandas as pd


# function to upload export data to GCP
def upload_to_bucket(blob_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""

    # Explicitly use service account credentials by specifying the private key file.
    storage_client = storage.Client.from_service_account_json(
        '../ods-nl-dnoi-creds.json')

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)

    #return bucket_name+'/'+blob_name


# login to the API with public and private key
pub_key = '877ec0175517fa7'
priv_key = 'E63$0D4596/FA97289B5'
client = MopinionClient(public_key=pub_key,
                        private_key=priv_key)

# GCP project & dataset
PROJECT_ID = 'ods-nl-dnoi'
dataset_id = 'Mopinion'

# get account details and resources from Mopinion and save it to account variable
account = client.resource("account")


# get all datasets available for mopinion and export them to CSV
df_datasets = pd.json_normalize(
    data=account.json(),
    record_path=['reports', 'dataSets'],
    # record_prefix='dataset.',
    meta=[['reports', 'name']],
    errors='ignore',)
df_datasets.set_index('id')
print('Retrieving account info and gathering all datasets')
#df_datasets.to_parquet('mopinion_datasets.parq', engine='fastparquet')
df_datasets.to_csv('mopinion_datasets.csv',index=False)
print('Export df_datasets to CSV...')

# get all dataset fields and export them to CSV
print('Retrieving all data field info...')
df_dataset_fields = None
for id in df_datasets['id']:
    field = client.resource("datasets", id, "fields")
    if df_dataset_fields is None:
        df_dataset_fields = pd.json_normalize(field.json(), record_path='data')
    else:
        df_dataset_field2 = pd.json_normalize(field.json(), record_path='data')
        #df_dataset_fields = df_dataset_fields.concat(df_dataset_field2, ignore_index=True)
        df_dataset_fields = pd.concat([df_dataset_fields, df_dataset_field2],
                  ignore_index=True)

#df_dataset_fields.to_parquet('mopinion_fields.parq', engine='fastparquet')
df_dataset_fields.to_csv('mopinion_fields.csv',index=False)
print('Export df_dataset_fields to CSV...')


# loop over all the datasets and fetch the ID for all datasets and consume all feedback
# data using pagination by the specified page limit below and concat all result and export them to CSV
print('Retrieving all feedback data info...(this might take a while)')
df_dataset_feedback = None
for id in df_datasets['id']:
    params = {"limit": 1000, "page": 1}
    has_more = True
    while has_more:
        feedback = client.request(f'/datasets/{id}/feedback', query_params=params)
        
        # if JSON response is successfull extract the required data from the JSON response
        if feedback.json().get('_meta').get('message') == 'OK':

            # if df_dataset_feedback dataframe does not exist create it
            if df_dataset_feedback is None:
                df_dataset_feedback = pd.json_normalize(
                    feedback.json(), record_path=['data', 'fields'],
                    meta=[['data', 'id'], ['data', 'created'],
                          ['data', 'report_id'], ['data', 'dataset_id']],
                    errors='ignore')
            # else concat the df_dataset_feedback2 to the main df_dataset_feedback dataframe
            else:
                df_dataset_feedback2 = pd.json_normalize(feedback.json(), record_path=['data', 'fields'],
                                                         meta=[['data', 'id'], ['data', 'created'],
                                                               ['data', 'report_id'], ['data', 'dataset_id']],
                                                         errors='ignore')

                #df_dataset_feedback = df_dataset_feedback.concat(df_dataset_feedback2, ignore_index=True)
                df_dataset_feedback = pd.concat([df_dataset_feedback, df_dataset_feedback2], ignore_index=True)
            # get the next page if available from the metadata

            has_more = feedback.json()['_meta']['has_more']
        else:
            has_more = False

        # provide an overview of the data fetch
        # Used for debugging
        #print(f'Mopinion dataset {id} page {params["page"]} has more {has_more}')

        # if there are more pages available for pagination retrieve the next page link and extract the number only
        if has_more:
            params['page'] = int(feedback.json()['_meta']['next'][feedback.json()[
                                 '_meta']['next'].rindex('=')+1:])

        

    print(f'df_dataset_feedback new length is {len(df_dataset_feedback)}')

# put columns in correct order
df_dataset_feedback = df_dataset_feedback[[
    'data.created', 'data.dataset_id', 'data.id', 'data.report_id', 'key', 'label', 'value']]

# Export all data gathered to df_dataset_feedback to CSV
df_dataset_feedback.to_csv('mopinion_feedback.csv',index=False)
print('Export df_dataset_feedback to CSV...')

# copy df_dataset_feedback to df1 and reset the index
df1 = df_dataset_feedback.reset_index(drop=True)

# make the value fields strings
df1['value'] = df1['value'].astype("string")

# remove duplicates and keep the last entry
df1 = df1.sort_values('value').drop_duplicates(
    subset=['data.id', 'label'], keep='last')



# go over all the dataset numbers within the df_dataset_feedback and export each dataset as separate pivot tables and export these to separate CSV files
# upload the CSV files to Google cloud storage
for dataset_nr in df_datasets['id']:
    df_export = df1[df1['data.dataset_id'] == dataset_nr].pivot(
        index=['data.dataset_id', 'data.id'], columns='label', values='value')
    
    if len(df_export) > 0:
            
        # upload each mopinion dataset pivot table to GCS
        source_file = f'{dataset_nr}.csv'
        df_export.to_csv(f'tmp/{source_file}')
        print(f'Uploading {source_file} to GCP bucket...')
        upload_to_bucket(blob_name=f'Decathlon_Netherlands/DataLake/External/mopinion_data/{source_file}',
                        bucket_name='decathlon_united_redshift_data', path_to_file=f'tmp/{source_file}')

        # create dataset for each mopinion dataset
        bq_client.create_table(f"{PROJECT_ID}.{dataset_id}.{dataset_nr}",exists_ok=True)

        table_id = str(dataset_nr)

        # tell the client everything it needs to know to upload our csv
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True
        job_config.allow_quoted_newlines=True
        job_config.write_disposition = "WRITE_TRUNCATE" #overwrite table with CSV

        # load the csv into bigquery
        with open(f'tmp/{source_file}', "rb") as file:
            job = bq_client.load_table_from_file(file, table_ref, job_config=job_config)

        job.result()  # Waits for table load to complete.

        # looks like everything worked :)
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))

        # Add mopinion dataset name as description to bigquery table from the dataset id
        table_ref = dataset_ref.table(table_id)
        table = bq_client.get_table(table_ref)  # API request to read current table config
        table.description = df_datasets[df_datasets['id']==dataset_nr].iloc[0]['name']
        
        #write back table description as update above
        table = bq_client.update_table(table, ["description"])

    else:
        print(f'Dataset {dataset_nr} has no data so will be skipped')

if __name__ == '__main__':
    print('')
    print('Uploading mopinion_datasets.csv to GCP bucket...')
    upload_to_bucket(blob_name='Decathlon_Netherlands/DataLake/External/mopinion_data/mopinion_datasets.csv', bucket_name='decathlon_united_redshift_data', path_to_file='mopinion_datasets.csv')
    print('Uploading mopinion_fields.csv to GCP bucket...')
    upload_to_bucket(blob_name='Decathlon_Netherlands/DataLake/External/mopinion_data/mopinion_fields.csv', bucket_name='decathlon_united_redshift_data', path_to_file='mopinion_fields.csv')
    print('Uploading mopinion_feedback.csv to GCP bucket...')
    upload_to_bucket(blob_name='Decathlon_Netherlands/DataLake/External/mopinion_data/mopinion_feedback.csv', bucket_name='decathlon_united_redshift_data',path_to_file='mopinion_feedback.csv')
    print('All Mopinion data have been retrieved and uploaded as CSV to Google Storage!')
    print('Job Done!')