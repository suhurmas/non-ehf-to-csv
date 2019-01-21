import requests
import sqlite3
import pandas
from io import StringIO

# Initialize DB
db = "ELMA-EHF.db"
conn = sqlite3.connect(db)
c = conn.cursor()

# Rebuild tables
c.execute("DROP TABLE IF EXISTS elma")
c.execute("DROP TABLE IF EXISTS elmaAddress")
c.execute('CREATE TABLE elma (identifier, name, EHF_CREDITNOTE_2_0, sanitized TEXT DEFAULT "no")')
conn.commit()

# Get csv ELMA list from difi
print("Fetching Difi ELMA participants...")
difi_result = requests.get('http://hotell.difi.no/download/difi/elma/participants?download')  # CSV file. ";" as seperator. Every value enclosed in ' " '
print("Difi ELMA participants downloaded successfully")

# Write CSV file to DB
# Insert rows in csv file using pandas lib
df = pandas.read_csv(StringIO(difi_result.text), delimiter=";")
# Dropping duplicte values, since difi_result contains something like 40K duplicates
df.sort_values("identifier", inplace = True)
df.drop_duplicates(subset ="identifier",
                     keep = False, inplace = True)
df[["identifier", "name", "EHF_CREDITNOTE_2_0"]].to_sql("elma", conn, if_exists='append', index=False) # keep only "identifier", "name", "EHF_CREDITNOTE_2_0"

# Select 100 first entries of ELMA users without EHF
c.execute('SELECT identifier, name FROM elma WHERE EHF_CREDITNOTE_2_0="Nei" LIMIT 100')
elma_ehf_no = c.fetchall()
conn.close() # Close since we're doing other OI now

# Download CSV file from brreg about each org in previous list
results = []
print("Fetching brreg info for 100 ELMA participants without EHF...")
for row in elma_ehf_no:
    # row[0] = orgnum/identifier
    brreg_result = requests.get('http://data.brreg.no/enhetsregisteret/enhet/%s.csv' % row[0])  # CSV file. ";" as seperator. Every value enclosed in ' " '

    if(brreg_result.status_code != 200):
        print("Error occured, could not find org: %s, skipping" % row[0])
        continue

    results.append(brreg_result.text)
    print("brreg ELMA participant %s, %s added" % (row[0], row[1]))
print(" %s brreg ELMA participants without EHF downloaded successfully" % len(results))

# Read previously fetched CSV, combine them into one pandas dataframe, with only relevant info
data_frame = pandas.DataFrame()
pandas.options.mode.chained_assignment = None  # Itterating like this creates warnings with pandas, this suppresses those warnings
for result in results:
    # Convert Brreg info to pandas data frame
    df = pandas.read_csv(StringIO(result), delimiter=";")
    # Add rows to data_frame
    data_frame = data_frame.append(df)
# Select which collums to keep
address_info = data_frame[["organisasjonsnummer","forretningsadresse.adresse","forretningsadresse.postnummer"
                 ,"forretningsadresse.poststed","forretningsadresse.landkode"
                 ,"forretningsadresse.land"]]
# Rename collumns
address_info.rename(columns=lambda x: x.replace("forretningsadresse.", ""), inplace=True)
address_info.rename(columns={"organisasjonsnummer":"identifier"}, inplace=True)

print(address_info)
print("After processing %s brreg results. We have %s entries ready to be inserted into db" % (len(results), len(address_info.index)))

# Reconnect to db and write dataframe to elmaAddress table
print("Writing addresses to db")
conn = sqlite3.connect(db)
c = conn.cursor()
# Write to table "elmaAddress". Bad practice.
address_info.to_sql("elmaAddress", conn, if_exists='append', index=False)
# Write to db and close connection
conn.commit()
# Mark added users as sanitized
c.execute('''UPDATE elma
SET sanitized = "yes"
WHERE identifier IN (SELECT identifier FROM elmaAddress)''')
conn.commit()
# Debugging
result = c.execute('''SELECT * FROM elmaAddress''')
print("We wrote %s rows from data frame to DB, and DB returned %s rows" % (len(data_frame.index), len(result.fetchall())))

# Get sanitized users and write their identifier, name and address to csv file
df = pandas.read_sql(sql='''SELECT elma.identifier, name, adresse, postnummer, poststed, landkode, land
FROM elma
INNER JOIN elmaAddress
ON elma.identifier=elmaAddress.identifier
WHERE elma.sanitized = "yes"''', con=conn)

print("Constructing csv with %s rows fetched from db" % len(df.index))
df.to_csv('NonEHFUsers.csv', index=False)

def getUsersWithoutEHF():
    conn = sqlite3.connect(db)
    c = conn.cursor()
    df = pandas.read_sql(sql='''SELECT *
    FROM elma
    INNER JOIN elmaAddress ON elma.identifier=elmaAddress.identifier''', con=conn)
    conn.close()
    return df
