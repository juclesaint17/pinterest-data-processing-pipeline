from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.cluster import ClusterMetadata



def create_topics():
        
    #create a client to adminstrate kafka broker
    admin_client = KafkaAdminClient(
        bootstrap_servers= "localhost:9092",
        client_id="kafka Administrator"
    )

    # list of topics
    topics =[]
    topics.append(NewTopic(name="Pinterest_Aicore_Topic",num_partitions=3,replication_factor=1))
    admin_client.create_topics(new_topics=topics)
    print(admin_client.list_topics())

create_topics()