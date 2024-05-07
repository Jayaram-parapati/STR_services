import boto3
import mimetypes
import os
from dotenv import load_dotenv
import json
from fastapi.responses import JSONResponse



class AWS_S3_Service:
    def __init__(self):
        load_dotenv()
        self.s3client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key"),
            region_name=os.getenv("region_name"),
        )

        self.S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
        

    def get_mime_type(self, file_extension):
        mime_type, _ = mimetypes.guess_type(f"file{file_extension}")
        return mime_type
    

    def upload_to_s3_object(self, file_content,unique_filename,content_type):
        try:
            print("file_content--->",file_content)
            file_content.seek(0)
            self.s3client.upload_fileobj(
                Fileobj=file_content,
                Bucket=self.S3_BUCKET_NAME,
                Key=unique_filename,
                ExtraArgs={"ContentType": content_type},
            )
            return {"messege": unique_filename,"status":200}
        except Exception as ex:
            # return JSONResponse({"messege": str(ex)})
            print("errro in upload_to_s3_file due to ", ex)
       
