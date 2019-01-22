from bottle import route, run, template, get, request
import sqlite3
import pandas
from io import StringIO

# Db info
db = "ELMA-EHF.db"

@get('/clients')            # matches /follow/defnull
def user_api():
    conn = sqlite3.connect(db)
    string = StringIO()
    postnummer = request.query.postnummer
    name = request.query.name
    adresse = request.query.adresse
    poststed = request.query.poststed
    land = request.query.land
    landkode = request.query.landkode
    identifier = request.query.identifier
    whereclauses = []
    if(len(postnummer) <= 4 and len(postnummer) > 0):
        whereclauses.append(" AND postnummer = {0} ".format(postnummer))
        print(" I see you've tried to fetch the users in area {0}, congratulations".format(postnummer))
    if(len(name) > 0):
        whereclauses.append("AND name LIKE '{0}''".format(name))
        print(" I see you've tried to fetch the user {0}, congratulations".format(name))
    if(len(adresse) > 0):
        whereclauses.append(" AND adresse LIKE '{0}' ".format(adresse))
        print(" I see you've tried to fetch the users at {0}, congratulations".format(adresse))
    if(len(poststed) > 0):
        whereclauses.append(" AND poststed LIKE '{0}' ".format(poststed))
        print(" I see you've tried to fetch the users at {0}, congratulations".format(poststed))
    if(len(land) > 0):
        whereclauses.append(" AND land LIKE '{0}' ".format(land))
        print(" I see you've tried to fetch the users at {0}, congratulations".format(land))
    if(len(landkode) > 0):
        whereclauses.append(" AND landkode LIKE '{0}' ".format(landkode))
        print(" I see you've tried to fetch the users at {0}, congratulations".format(landkode))


    df = pandas.read_sql(sql='''SELECT elma.identifier, name, adresse, postnummer, poststed, landkode, land
                                FROM elma
                                INNER JOIN elmaAddress
                                ON elma.identifier=elmaAddress.identifier
                                WHERE elma.sanitized = "yes"{0};'''.format("".join(whereclauses)), con=conn)
    df.to_json(string, orient="records")
    return string.getvalue()

run(host='localhost', port=8080)
