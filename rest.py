from bottle import route, run, template, get, request, response
import sqlite3
import pandas
from io import StringIO

# Db info
db = "ELMA-EHF.db"

@get('/clients')
def clients():
    # Connect to db
    conn = sqlite3.connect(db)
    # Pandas expects to write to IO, this is a hack to get the response string
    response_string = StringIO()
    # Parameters we fetch through get params "?=''"
    postnummer = request.query.postnummer
    name = request.query.name
    adresse = request.query.adresse
    poststed = request.query.poststed
    land = request.query.land
    landkode = request.query.landkode
    identifier = request.query.identifier
    encoding = request.query.encoding
    # List to store individual clauses for the final sql query
    whereclauses = []

    # Constructing where clauses for the final sql statement
    if(len(postnummer) <= 4 and len(postnummer) > 0):
        whereclauses.append(' AND postnummer = {0} '.format(postnummer))
    if(len(name) > 0):
        whereclauses.append(' AND name LIKE "{0}"'.format(name))
    if(len(adresse) > 0):
        whereclauses.append(' AND adresse LIKE "{0}" '.format(adresse))
    if(len(poststed) > 0):
        whereclauses.append(' AND poststed LIKE "{0}" '.format(poststed))
    if(len(land) > 0):
        whereclauses.append(' AND land LIKE "{0}" '.format(land))
    if(len(landkode) > 0):
        whereclauses.append(' AND landkode LIKE "{0}" '.format(landkode))
    if(len(identifier) == 9):
        whereclauses.append(' AND elma.identifier = {0} '.format(identifier))

    # Final sql statement
    df = pandas.read_sql(sql='''SELECT elma.identifier, name, adresse, postnummer, poststed, landkode, land
                                FROM elma
                                INNER JOIN elmaAddress
                                ON elma.identifier=elmaAddress.identifier
                                WHERE elma.sanitized = "yes"{0};'''.format("".join(whereclauses)), con=conn)
    # Set encoding
    if (encoding.lower() == "csv"):
        df.to_csv(response_string, index=False)
        response.content_type = 'text/csv; charset=utf-8'
    elif (encoding.lower() == "html"):
        response_string = StringIO((df.to_html()))
        response.content_type = 'text/html; charset=utf-8'
    else:
        df.to_json(response_string, orient="records")
        response.content_type = 'text/json; charset=utf-8'
    # Return sql result as encoded string
    return response_string.getvalue()

#Start server
run(host='localhost', port=8080)
