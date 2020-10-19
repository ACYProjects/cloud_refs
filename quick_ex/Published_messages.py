from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
# topic_path = publisher.topic_path("myprojectID", "test")
topic_path = `projects/pubsub-public-data/topics/taxirides-realtime`

for n in range(1, 100):
    data = u"Message number {}".format(n)
    # Data must be a bytestring
    data = data.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data=data)
    print(future.result())

print("Published messages.")
