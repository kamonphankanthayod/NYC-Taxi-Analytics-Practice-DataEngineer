import streamlit as st
import boto3
import pandas as pd
import time

# --- CONFIGURATION ---
# ตั้งค่าให้ตรงกับ AWS Account ของคุณ
DATABASE_NAME = 'nyctaxi_dev_db_catalog'
TABLE_NAME = 'processed-all_rides'
S3_OUTPUT_BUCKET = 's3://nyctaxi-dev-datalake-0116/athena/'
AWS_REGION = 'ap-southeast-1'

# --- PAGE SETUP ---
st.set_page_config(page_title="NYC Taxi Analytics", layout="wide")

st.title("🚖 NYC Taxi Data Platform")
st.markdown("Dashboard สรุปข้อมูลการเดินทาง Taxi ใน New York (Data Pipeline by Airflow)")

# --- SIDEBAR (เมนูเลือก) ---
with st.sidebar:
    st.header("📅 ตัวเลือกข้อมูล")
    selected_year = st.selectbox("เลือกปี", ["2024"])
    selected_month = st.selectbox("เลือกเดือน", ["January", "February", "March"])
    
    st.markdown("---")
    st.caption("สถานะระบบ: 🟢 Online")

# --- FUNCTIONS ---
def query_athena(query):
    """ฟังก์ชันสำหรับยิง SQL ไปหา Athena"""
    client = boto3.client('athena', region_name=AWS_REGION)
    
    # 1. ส่งคำสั่ง Query
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE_NAME},
        ResultConfiguration={'OutputLocation': S3_OUTPUT_BUCKET}
    )
    query_execution_id = response['QueryExecutionId']
    
    # 2. รอผลลัพธ์ (Polling)
    with st.spinner('กำลังดึงข้อมูลจาก Data Lake...'):
        while True:
            stats = client.get_query_execution(QueryExecutionId=query_execution_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)
    
    # 3. อ่านผลลัพธ์
    if status == 'SUCCEEDED':
        results = client.get_query_results(QueryExecutionId=query_execution_id)
        # แปลงผลลัพธ์เป็น Pandas DataFrame (แบบย่อ)
        # หมายเหตุ: วิธีนี้เหมาะกับข้อมูลไม่เยอะมาก ถ้าเยอะควรใช้ s3 select หรือ awswrangler
        columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        for row in results['ResultSet']['Rows'][1:]:
            rows.append([field.get('VarCharValue', None) for field in row['Data']])
        
        df = pd.DataFrame(rows, columns=columns)
        return df
    else:
        # ดึงสาเหตุความผิดพลาดออกมาโชว์
        error_reason = stats['QueryExecution']['Status'].get('StateChangeReason', 'Unknown Error')
        st.error(f"Query Failed: {status}")
        st.error(f"Reason: {error_reason}") # <--- บรรทัดนี้จะบอกใบ้เรา
        return None

# --- MAIN CONTENT ---

# สร้าง Tab เพื่อแยกส่วนดูข้อมูล กับ ส่วนสั่งงาน
tab1, tab2 = st.tabs(["📊 Analytics Dashboard", "⚙️ Control Plane"])

with tab1:
    st.subheader(f"สรุปข้อมูลประจำเดือน: {selected_month} {selected_year}")
    
    if st.button("🔄 โหลดข้อมูลล่าสุด", type="primary"):
        # SQL Query (ดึงข้อมูลสรุป)
        sql = f"""
            SELECT type, count(*) as rides 
            FROM "{DATABASE_NAME}"."{TABLE_NAME}" 
            GROUP BY type 
            ORDER BY rides DESC
        """
        
        df = query_athena(sql)
        
        if df is not None and not df.empty:
            # แปลงข้อมูลตัวเลข
            df['rides'] = df['rides'].astype(int)
            
            # แสดง Metric
            total_rides = df['rides'].sum()
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Rides", f"{total_rides:,}")
            col2.metric("Top Type", df.iloc[0]['type'])
            col3.metric("Data Source", "Amazon S3 (Parquet)")
            
            # แสดงกราฟและตาราง
            c1, c2 = st.columns([2, 1])
            with c1:
                st.caption("จำนวนเที่ยวแบ่งตามประเภทรถ")
                st.bar_chart(df.set_index('type'), color="#FF4B4B")
            with c2:
                st.caption("ตารางข้อมูลดิบ")
                st.dataframe(df, hide_index=True)
        else:
            st.warning("ไม่พบข้อมูล หรือยังไม่ได้รัน Pipeline")

with tab2:
    st.subheader("สั่งงาน Data Pipeline (Airflow)")
    st.info("ฟีเจอร์นี้จะเชื่อมต่อกับ Airflow API เพื่อสั่ง Trigger DAG สำหรับเดือนใหม่ (Coming Soon)")
    if st.button("▶️ Start Pipeline for selected month"):
        st.toast("คำสั่งถูกส่งไปยัง Airflow แล้ว! (Simulation)")