import producer_server


def run_kafka_server():
    
	# TODO get the json file path
    input_file = "police-department-calls-for-service.json"
    topic = "com.udacity.sf.crime.calls"
    bootstrap_servers = "localhost:9092"
    client_id = "pd.calls"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        client_id=client_id
    )

    return producer

def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
