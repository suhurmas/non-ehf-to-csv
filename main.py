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

def writeELMAtoDB(csv_file):
    #https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python
    #  Connect to local db
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS elma")

    c.execute("CREATE TABLE elma (identifier, name, EHF_CREDITNOTE_2_0, sanitized)")
    # Insert a row of data
    df = pandas.read_csv(csv_file, delimiter=";")
    print(df)
    df[["identifier", "name", "EHF_CREDITNOTE_2_0"]].to_sql("elma", conn, if_exists='append', index=False)
    conn.commit()
    # Do this instead
    c.execute('SELECT * FROM elma')
    print(c.fetchone())
    conn.close()

def findNonEHF():
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute('SELECT identifier FROM elma WHERE EHF_CREDITNOTE_2_0="Nei" LIMIT 100')
    #print(c.fetchall())
    result = c.fetchall()
    conn.close()
    return result

def fetchNonEHFAddress(rows):
    print("Fetching Addresses for 100 ELMA participants without EHF...")
    results = []
    for row in rows:
        brreg_result = requests.get('http://data.brreg.no/enhetsregisteret/enhet/%s.csv' % row[0])  # CSV file. ";" as seperator. Every value enclosed in ' " '
        if(brreg_result.status_code != 200):
            print("Error occured, skipping")
            break #break for debugging purposes
        results.append(brreg_result.text)  # Crash on failure
        print("brreg ELMA participants without EHF downloaded successfully")

    for result in results:
        df = pandas.read_csv(StringIO(result), delimiter=";")
        #print(df)
        address_info = df[["organisasjonsnummer","forretningsadresse.adresse","forretningsadresse.postnummer"
                         ,"forretningsadresse.poststed","forretningsadresse.landkode"
                         ,"forretningsadresse.land"]]
        address_info.rename(columns=lambda x: x.replace("forretningsadresse.", ""), inplace=True)
        address_info.rename(columns={"organisasjonsnummer":"identifier"}, inplace=True)
        #print(address_info.to_dict("records"))
        print(address_info)
        conn = sqlite3.connect(db)
        c = conn.cursor()
        address_info.to_sql("elmaAddress", conn, if_exists='append', index=False)
        conn.commit()

        c.execute('''SELECT *
        FROM elma
        INNER JOIN elmaAddress ON elma.identifier=elmaAddress.identifier''')
        print(c.fetchall())
        conn.close()

    return address_info.to_dict("records")
# Main
'''
elma_list = fetchELMAList()
writeELMAtoDB(StringIO(elma_list))
'''
print(fetchNonEHFAddress(findNonEHF()))  # Returns a list of row objects
