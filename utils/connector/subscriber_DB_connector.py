import psycopg

def subscriberDB_connector(js):

    #init subscriber database connection
    conn = psycopg.connect(
            host=js['host'],
            dbname=js['dbname'],
            user=js['user'],
            password=js['password'])

    return conn


if __name__ == "__main__":
    
    import json

    #get credentials
    with open("creds.json") as f:
        data=f.read()
    js = json.loads(data)

    subscriberDB_connector(js = js)