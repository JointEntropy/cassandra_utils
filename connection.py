from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Session
import json


def spawn_session(bundle_path: str, tokens_path: str) -> Session:    
    cloud_config= {'secure_connect_bundle': bundle_path}
    with open('influencers-token.json', 'r') as f:
        tokens_info = json.load(f)
        
    auth_provider = PlainTextAuthProvider(tokens_info['clientId'], tokens_info['secret'])
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    return session

def simple_session_test(session: Session):
    row = session.execute("select release_version from system.local").one()
    if row:
        print(row[0])
    else:
        print("An error occurred.")

