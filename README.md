# non-ehf-to-csv
Writes addresses of orgs without EHF to CSV file.
Also exposes address-DB via a REST api


To run:    
pip install requirements.txt    


nonehftocsv.py    
Constructs db and writes ~100 addresses to csv file.     


rest.py    
Creates a very simple GET api to acccess the DB. Returns a json string.    
usage:    
localhost:8080/clients?params=value    
list of params:    
        postnummer    
        name    
        adresse    
        poststed    
        land    
        landkode    
        identifier    
        encoding:    
                json - default    
                csv    
                html    
No params list entire address list.
