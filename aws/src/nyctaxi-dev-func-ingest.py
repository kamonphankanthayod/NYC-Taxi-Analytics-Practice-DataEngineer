# ingest data มา **ทีละเดือน** ของ ทั้ง 4 ประเภท
import boto3
import urllib.request
import json

# --- Config ---
S3_BUCKET = "mini-challenge-taxitype-0116"
TARGET_PREFIX = "raw"

# --- ตัวแปร ---
YEAR_MONTH = "2024-01"     ########################### กำหนด เดือน และ ปี ได้ที่นี่ ############################################

# ดึง "ปี" ออกมาจาก YEAR_MONTH
# ใช้ 'split' เพื่อแยก "2024-01" ที่เครื่องหมาย - แล้วเอาตัวแรก
YEAR = YEAR_MONTH.split('-')[0]

# [cite_start]รายการประเภท taxi
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

# URL พื้นฐาน
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print(f"Starting ingestion process for {YEAR_MONTH}...")
    
    results = []

    # วนลูปตามประเภท taxi
    for taxi_type in TAXI_TYPES:
        
        # สร้างชื่อไฟล์และ URL แบบไดนามิก
        filename = f"{taxi_type}_tripdata_{YEAR_MONTH}.parquet"
        url = f"{BASE_URL}/{filename}"
        
        s3_key = f"{TARGET_PREFIX}/{YEAR}/{taxi_type}/{filename}"
        
        print(f"Processing: {filename}")
        print(f"  -> Source URL: {url}")
        print(f"  -> Target S3 Key: {s3_key}")

        try:
            with urllib.request.urlopen(url) as response:
                s3_client.upload_fileobj(response, S3_BUCKET, s3_key)
            
            message = f"SUCCESS: Uploaded {filename} to s3://{S3_BUCKET}/{s3_key}"
            print(message)
            results.append(message)

        except Exception as e:
            message = f"ERROR: Failed to process {filename}. Error: {str(e)}"
            print(message)
            results.append(message)
    
    print("Ingestion process finished.")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Ingestion for {YEAR_MONTH} finished.',
            'upload_results': results
        })
    }