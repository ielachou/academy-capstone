import load_data
import retrieve_sf_credentials as psc
import snowflake_connect as sfc
import json

if __name__ == "__main__":
    df = load_data.load()
    credentials = psc.get_creds()
    sfc.write_to_sf(json.loads(credentials), df)
    print(credentials)