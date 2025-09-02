import boto3, os
from typing import List, Dict
from automate.credentials import s3_info
from automate.logger import getLogger

class S3Downloader:
    def __init__(self, credentails: dict=None):
        self.credentials = s3_info
        self.logger = getLogger("S3Downloader", Stream=True)

    def connects3(self):
        credentials = self.credentials
        aws_access_key_id = credentials.get('access_key', '')
        aws_secret_access_key = credentials.get('secret_access_key', '')
        region_name = credentials.get('region', '')
        try:
            s3 = boto3.client('s3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name)
            self.logger.info("Connected to S3 successfully")
            return True, s3
        except Exception as e:
            self.logger.error(f"Error connecting S3: {str(e)}")
            return False, None

    def downloads3folder(self, s3folder, localdirectory):
        s3status, s3 = self.connects3()
        if s3status:
            file_metadata = []
            bucketname = self.credentials["bucket_name"]
            try:
                # Create the local folder path based on the S3 folder name
                folder_name = os.path.basename(os.path.normpath(s3folder))
                local_folder = os.path.join(localdirectory, folder_name)
                os.makedirs(local_folder, exist_ok=True)
                self.logger.info(f"Downloading {s3folder} to {localdirectory}")
                # List objects in the specified folder
                response = s3.list_objects_v2(Bucket=bucketname, Prefix=s3folder)
                
                if 'Contents' not in response:
                    self.logger.warning(f"No files found in S3 folder: {s3folder}")
                    return file_metadata

                for obj in response['Contents']:
                    s3_key = obj['Key']
                    filename = os.path.basename(s3_key)
                    if not filename:
                        continue  # Skip folder keys

                    local_path = os.path.join(local_folder, filename)
                    
                    # Download file
                    self.logger.info(f"Downloading: {s3_key} to {os.path.basename(local_path)}")
                    s3.download_file(bucketname, s3_key, local_path)

                    # Track file metadata
                    file_metadata.append({
                        'filename': filename,
                        'local_path': local_path
                    })

                self.logger.info(f"Download complete for {s3folder}")
            except Exception as e:
                self.logger.error(f"Error downloading {s3folder} files from S3: {str(e)}")

            return file_metadata



if __name__ == '__main__':
    S3Downloader().downloads3folder(s3folder='Output/12_12_2024', localdirectory='C:\\Users\\Rupesh.Ranjan\\OneDrive - GlobalData PLC\\Desktop\\Nexgile_Tasks\\nexgilereviewtemplate-new')
    





