import requests
import sqlite3
import pandas
from io import StringIO
db = "example.db"
test_csv = StringIO( '''"EHF_ORDER_RESPONSE";"EHF_CATALOGUE_RESPONSE";"EHF_INVOICE_CREDITNOTE_2_0";"BIS36_MLR";"PAYMENT_02_RESPONSE";"PEPPOLBIS_03A_2_0";"PEPPOLBIS_28A_2_0";"PEPPOLBIS_30A_2_0";"BIS04_V2";"identifier";"EHF_CREDITNOTE_2_0";"EHF_ORDER";"PEPPOLBIS_3_0_BILLING_01_CII";"PEPPOLBIS_28A_2_0_RESPONSE";"BIS01";"PEPPOLBIS_3_0_BILLING_01_UBL";"Icd";"BIS03";"PAYMENT_01_RESPONSE";"EHF_30A_1_0";"EHF_XYA_1_0_REMINDER";"BIS06";"ISO20022_Pain_001";"PAYMENT_02";"PAYMENT_01";"name";"regdate";"EHF_INVOICE_2_0";"EHF_CATALOGUE";"BIS05_V2";"PEPPOLBIS_01A_2_0"
"Nei";"Nei";"Ja";"Nei";"Nei";"Nei";"Nei";"Nei";"Ja";"966261218";"Ja";"Nei";"Nei";"Nei";"Nei";"Nei";"9908";"Nei";"Nei";"Nei";"Nei";"Nei";"Nei";"Nei";"Nei";"BØMLO FOLKEHØGSKULE";"2014.04.07";"Ja";"Nei";"Ja";"Nei"
"Nei";"Nei";"Ja";"Nei";"Nei";"Nei";"Nei";"Nei";"Ja";"999145272";"Ja";"Nei";"Nei";"Nei";"Nei";"Nei";"9908";"Nei";"Nei";"Nei";"Nei";"Nei";"Nei";"Nei";"Nei";"RF EIENDOMSFORVALTNING AS";"2013.10.22";"Ja";"Nei";"Ja";"Nei"
"Ja";"Nei";"Ja";"Nei";"Nei";"Nei";"Nei";"Nei";"Ja";"961381096";"Ja";"Nei";"Nei";"Ja";"Nei";"Nei";"9908";"Nei";"Nei";"Nei";"Nei";"Nei";"Nei";"Nei";"Nei";"SKJÅK KOMMUNE";"2013.02.25";"Ja";"Ja";"Ja";"Ja"'''
)
def fetchELMAList():
    print("Fetching Difi ELMA participants...")
    difi_result = requests.get('http://hotell.difi.no/download/difi/elma/participants?download')  # CSV file. ";" as seperator. Every value enclosed in ' " '
    difi_result.raise_for_status()  # Crash on failure
    print("Difi ELMA participants downloaded successfully")
    return difi_result.text

def writeELMAtoDB(csv_string):
    # https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python
    # Connect to local db
    conn = sqlite3.connect(db)
    c = conn.cursor()
    # Rebuild tables
    c.execute("DROP TABLE IF EXISTS elma")
    c.execute("DROP TABLE IF EXISTS elmaAddress")
    c.execute("CREATE TABLE elma (identifier, name, EHF_CREDITNOTE_2_0, sanitized)")
    # Insert rows in csv file using pandas lib
    df = pandas.read_csv(StringIO(csv_string), delimiter=";")
    df[["identifier", "name", "EHF_CREDITNOTE_2_0"]].to_sql("elma", conn, if_exists='append', index=False)
    # Write to db and close connection
    conn.commit()
    conn.close()

def findNonEHF():
    # Connect to local db
    conn = sqlite3.connect(db)
    c = conn.cursor()
    # Select 100 first entries of ELMA users without EHF
    c.execute('SELECT identifier FROM elma WHERE EHF_CREDITNOTE_2_0="Nei" LIMIT 100')
    # Fetch result and close connection. Returns result
    result = c.fetchall()
    conn.close()
    return result

def fetchNonEHFBrregInfo(rows):
    results = []

    print("Fetching brreg info for 100 ELMA participants without EHF...")
    for row in rows:
        brreg_result = requests.get('http://data.brreg.no/enhetsregisteret/enhet/%s.csv' % row[0])  # CSV file. ";" as seperator. Every value enclosed in ' " '

        if(brreg_result.status_code != 200):
            print("Error occured, skipping")
            continue

        results.append(brreg_result.text)
        print("brreg ELMA participant added")

    print(" %s brreg ELMA participants without EHF downloaded successfully" % len(results))
    return results

def fetchAndWriteAddressFromBrregResults(results):
    for result in results:
        # Convert Brreg info to pandas data frame
        df = pandas.read_csv(StringIO(result), delimiter=";")
        # Select which collums to keep
        address_info = df[["organisasjonsnummer","forretningsadresse.adresse","forretningsadresse.postnummer"
                         ,"forretningsadresse.poststed","forretningsadresse.landkode"
                         ,"forretningsadresse.land"]]
        # Rename collumns
        address_info.rename(columns=lambda x: x.replace("forretningsadresse.", ""), inplace=True)
        address_info.rename(columns={"organisasjonsnummer":"identifier"}, inplace=True)

        writeAddressInfoToDb(address_info)

def writeAddressInfoToDb(address_info):
    print("Writing address to db")
    conn = sqlite3.connect(db)
    c = conn.cursor()
    # Write to table "elmaAddress". Bad practice.
    address_info.to_sql("elmaAddress", conn, if_exists='append', index=False)
    # Write to db and close connection
    conn.commit()
    conn.close()

def getUsersWithoutEHF():
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute('''SELECT *
    FROM elma
    INNER JOIN elmaAddress ON elma.identifier=elmaAddress.identifier''')
    result = c.fetchall()
    conn.close()
    return result

def writeUsersWithoutEHFToCSV():
    conn = sqlite3.connect(db)
    df = pandas.read_sql(sql='''SELECT elma.identifier, name, adresse, postnummer, poststed, landkode, land
    FROM elma
    INNER JOIN elmaAddress ON elma.identifier=elmaAddress.identifier''', con=conn)
    df.to_csv('NonEHfUsers.csv', index=False)
# Main
# Get ELMA users from DIFI and wirte to DB
elma_list = fetchELMAList()
writeELMAtoDB(elma_list)

# Get non EHF users from DB, get their info from brreg and write address to db
non_EHF = findNonEHF()
non_EHF_brreg_info = fetchNonEHFBrregInfo(non_EHF)
fetchAndWriteAddressFromBrregResults(non_EHF_brreg_info)

# Find the users in DB without ehf and their addesses
print(getUsersWithoutEHF())
writeUsersWithoutEHFToCSV()
