from kafka import KafkaClient
from kafka.cluster import ClusterMetadata


def retrieve_metadata():

    meta_cluster_conn = ClusterMetadata(
        bootstrap_servers="localhost:9092",
    )
    print(meta_cluster_conn.brokers())


def kafka_broker_connect():

    client = KafkaClient(
        bootstrap_server="localhost:9092",
        client_id="Broker test"
    )
    print(client.bootstrap_connected())
    print(client.check_version())


retrieve_metadata()
kafka_broker_connect()