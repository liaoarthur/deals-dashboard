import os
from databricks import sql
from dotenv import load_dotenv

# Load your credentials
load_dotenv()

print("="*50)
print("TESTING DATABRICKS CONNECTION")
print("="*50)

# Show what we're using (without revealing the full token)
hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
token = os.getenv("DATABRICKS_TOKEN")

print(f"\nHostname: {hostname}")
print(f"HTTP Path: {http_path}")
print(f"Token: {'✓ Found' if token else '✗ Missing'}")
print()

# Try to connect
try:
    print("Connecting to Databricks...")
    conn = sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=token
    )
    
    print("✓ Connected successfully!")
    
    # Try a simple query
    print("\nTesting query...")
    cursor = conn.cursor()
    cursor.execute("SELECT 'Hello from Databricks!' as message")
    result = cursor.fetchone()
    
    print(f"✓ Query successful: {result[0]}")
    
    # List your definitive tables
    print("\n" + "="*50)
    print("LOOKING FOR DEFINITIVE TABLES")
    print("="*50 + "\n")
    
    # Check ad_hoc schema
    print("Checking ad_hoc schema...")
    cursor.execute("SHOW TABLES IN prod_analytics_global.ad_hoc")
    tables = cursor.fetchall()
    
    definitive_tables = []
    for table in tables:
        table_name = table[1] if len(table) > 1 else table[0]
        if 'definitive' in table_name.lower():
            definitive_tables.append(f"prod_analytics_global.ad_hoc.{table_name}")
            print(f"  ✓ Found: {table_name}")
    
    # Check exposure schema
    print("\nChecking exposure schema...")
    cursor.execute("SHOW TABLES IN prod_analytics_global.exposure")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[1] if len(table) > 1 else table[0]
        if 'definitive' in table_name.lower():
            definitive_tables.append(f"prod_analytics_global.exposure.{table_name}")
            print(f"  ✓ Found: {table_name}")
    
    print(f"\n✅ SUCCESS! Found {len(definitive_tables)} definitive tables")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
    print("\nTroubleshooting:")
    print("1. Check your .env file has the correct values")
    print("2. Make sure your SQL Warehouse is 'Started' in Databricks")
    print("3. Make sure your token hasn't expired")