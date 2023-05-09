import configparser
import boto3
import os

config = configparser.ConfigParser()
config.read('dl.cfg')

s3 = boto3.client('s3', aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
                  aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'])

def upload_to_s3(source_directory, bucket_name):
    for root, dirs, files in os.walk(source_directory):
        for file in files:
            local_path = os.path.join(root, file)
            s3_path = os.path.relpath(local_path, source_directory)
            s3.upload_file(local_path, bucket_name, s3_path)
            print(f'Uploaded {local_path} to S3://{bucket_name}/{s3_path}')

def main():
    
    upload_to_s3('../../data/18-83510-I94-Data-2016/', 'melihburakmert-i94-immigration-data-2')
    upload_to_s3('us-cities-demographics/', 'melihburakmert-us-cities-demographics-2')
    upload_to_s3('../../data2/', 'melihburakmert-temperature-data-2')
    


if __name__ == "__main__":
    main()