import load_data
import retrieve_sf_credentials as psc

if __name__ == "__main__":
    df = load_data.load()
    credentials = psc.get_creds()
    print(credentials)