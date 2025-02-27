import botocore 
import botocore.session 
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 


def get_creds():
    client = botocore.session.get_session().create_client('secretsmanager')
    cache_config = SecretCacheConfig()
    cache = SecretCache( config = cache_config, client = client)

    secret = cache.get_secret_string('snowflake/capstone/login')
    return secret