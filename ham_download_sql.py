import requests
from ftplib import FTP
import os
import zipfile

# ---------------------------
# Config
# ---------------------------

urls = [
    "https://data.fcc.gov/download/pub/uls/complete/l_am.zip",
    "ftp://wirelessftp.fcc.gov/pub/uls/complete/l_amat.zip",
    "ftp://wirelessftp.fcc.gov/pub/uls/complete/l_am.zip",
    "https://data.fcc.gov/download/pub/uls/complete/amateur.zip"
]

script_dir = os.path.dirname(os.path.abspath(__file__))
zip_path = os.path.join(script_dir, "amateur.zip")
sql_path = os.path.join(script_dir, "amateur_calls.sql")

FIELDS = [
    "record_type","unique_system_identifier","uls_file_number","ebf_number",
    "call_sign","status_code","status_date","name","first_name",
    "middle_initial","last_name","suffix","phone","fax","email",
    "street_address","city","state","zip_code","po_box","attn_line",
    "frn","registration_number","license_status","reserved1","reserved2",
    "reserved3","reserved4"
]

BATCH_SIZE = 5000

# ---------------------------
# Download helpers
# ---------------------------

def download_http(url):
    print(f"Trying HTTPS: {url}")
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    return True

def download_ftp(url):
    print(f"Trying FTP: {url}")
    parts = url.replace("ftp://","").split("/",1)
    host = parts[0]
    path = parts[1]
    ftp = FTP(host, timeout=120)
    ftp.login()
    with open(zip_path,"wb") as f:
        ftp.retrbinary(f"RETR {path}", f.write)
    ftp.quit()
    return True

# ---------------------------
# Download file
# ---------------------------

for u in urls:
    try:
        if u.startswith("ftp://"):
            if download_ftp(u):
                break
        else:
            if download_http(u):
                break
    except Exception as e:
        print(f"Failed {u}: {e}")
        if os.path.exists(zip_path):
            os.remove(zip_path)
else:
    print("All download attempts failed.")
    exit(1)

# ---------------------------
# Extract EN.dat
# ---------------------------

with zipfile.ZipFile(zip_path,"r") as z:
    en_file = None
    for f in z.namelist():
        if f.lower().startswith("en") and f.lower().endswith(".dat"):
            en_file = f
            z.extract(f,script_dir)
            break

#os.remove(zip_path)

if not en_file:
    print("EN.dat not found.")
    exit(1)

data_file = os.path.join(script_dir,en_file)
print("Processing",data_file)

# ---------------------------
# SQL helper
# ---------------------------

def esc(s):
    return s.replace("'","''")

# ---------------------------
# Track unique values and assign IDs
# ---------------------------

unique_states = {}
unique_cities = {}
unique_zips = {}

state_id = 1
city_id = 1
zip_id = 1

batch = []

with open(sql_path,"w",encoding="utf-8") as sql:

    sql.write("BEGIN TRANSACTION;\n")

    # Create tables
    sql.write("""
CREATE TABLE IF NOT EXISTS states (
    id INTEGER PRIMARY KEY,
    state TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS cities (
    id INTEGER PRIMARY KEY,
    city TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS zips (
    id INTEGER PRIMARY KEY,
    zip TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS amateur_calls (
    call_sign TEXT PRIMARY KEY,
    name TEXT,
    street TEXT,
    city_id INTEGER,
    state_id INTEGER,
    zip_id INTEGER,
    frn TEXT,
    FOREIGN KEY(city_id) REFERENCES cities(id),
    FOREIGN KEY(state_id) REFERENCES states(id),
    FOREIGN KEY(zip_id) REFERENCES zips(id)
);
""")

    with open(data_file,"r",encoding="latin-1") as infile:

        for line in infile:

            parts = [p.strip() for p in line.strip().split("|")]

            if len(parts) < len(FIELDS):
                parts += [""] * (len(FIELDS) - len(parts))

            record = dict(zip(FIELDS,parts))

            call = record.get("call_sign","")
            if not call:
                continue

            name = record.get("name") or f"{record.get('first_name','')} {record.get('last_name','')}".strip()
            street = record.get("street_address","")
            city = record.get("city","")
            state = record.get("state","")
            zip_code = record.get("zip_code","")[:5]
            frn = record.get("frn","")

            # Map to unique IDs
            if state:
                if state not in unique_states:
                    unique_states[state] = state_id
                    state_id += 1
                s_id = unique_states[state]
            else:
                s_id = "NULL"

            if city:
                if city not in unique_cities:
                    unique_cities[city] = city_id
                    city_id += 1
                c_id = unique_cities[city]
            else:
                c_id = "NULL"

            if zip_code:
                if zip_code not in unique_zips:
                    unique_zips[zip_code] = zip_id
                    zip_id += 1
                z_id = unique_zips[zip_code]
            else:
                z_id = "NULL"

            batch.append(
                "('{call}','{name}','{street}',{c_id},{s_id},{z_id},'{frn}')".format(
                    call=esc(call),
                    name=esc(name),
                    street=esc(street),
                    c_id=c_id,
                    s_id=s_id,
                    z_id=z_id,
                    frn=esc(frn)
                )
            )

            if len(batch) >= BATCH_SIZE:
                sql.write(
                    "INSERT OR REPLACE INTO amateur_calls "
                    "(call_sign,name,street,city_id,state_id,zip_id,frn) VALUES\n"
                )
                sql.write(",\n".join(batch))
                sql.write(";\n")
                batch.clear()

        if batch:
            sql.write(
                "INSERT OR REPLACE INTO amateur_calls "
                "(call_sign,name,street,city_id,state_id,zip_id,frn) VALUES\n"
            )
            sql.write(",\n".join(batch))
            sql.write(";\n")

    # Insert unique values into lookup tables
    if unique_states:
        sql.write("INSERT OR IGNORE INTO states(id,state) VALUES\n")
        sql.write(",\n".join(f"({i},'{esc(s)}')" for s,i in unique_states.items()))
        sql.write(";\n")

    if unique_cities:
        sql.write("INSERT OR IGNORE INTO cities(id,city) VALUES\n")
        sql.write(",\n".join(f"({i},'{esc(c)}')" for c,i in unique_cities.items()))
        sql.write(";\n")

    if unique_zips:
        sql.write("INSERT OR IGNORE INTO zips(id,zip) VALUES\n")
        sql.write(",\n".join(f"({i},'{esc(z)}')" for z,i in unique_zips.items()))
        sql.write(";\n")

    # Indexes on foreign keys
    sql.write("""
CREATE INDEX IF NOT EXISTS idx_amateur_state ON amateur_calls(state_id);
CREATE INDEX IF NOT EXISTS idx_amateur_zip ON amateur_calls(zip_id);
CREATE INDEX IF NOT EXISTS idx_amateur_city ON amateur_calls(city_id);
""")

    sql.write("COMMIT;\n")

print("SQL written to:", sql_path)