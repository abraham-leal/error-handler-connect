# Error Handling Connector

The Error Handling Connector aims to ease the burden of common error handling patterns for Kafka Connect.

This connector has two modes:

-  `RESEND` : This mode finds the reported record and resends it without modification (except for an additional metadata header) to the original topic.
   This assures this record is in the same partition and will be reprocessed in the future.
-  `REMAP`  : This mode finds the reported record and sends it to a different topic without modification.
    This mode also ensures the record will live in the same partition number as the source as long as the destination has the same number of partitions.
   
The connector is meant to listen to Kafka's "dlq" topics generated by the Kafka Connect framework due to tolerances, as well as Confluent's Connect Reporter.
It is also possible to define custom headers to seek in order to find the necessary metadata to resend or remap the record.

## Configuration

The following properties are accepted in the connector definition:

```
topics="Comma delimited list of topics to listen for error reports"
mode="Mode to run this connector on. Supported: RESEND || REMAP. Default: RESEND"
topics.destinations="Only needed when mode=RESEND. List of topics to remap records to. The mapping will be in the same order as the topics appear in the `topics` configuration. Therefore, this list must have the same number of entries as `topics`."
error.handler.retries="Amount of times the connector should resend a message if it sees it errored again"
final.topic.no.retries="Topic to send the message to when retries are exhausted"
max.seeking.block="Maximum amount of time the connector will wait to find the errored record"
custom.topic.header.field.name="Custom header in the error message to extract the originating topic from"
custom.partition.header.field.name="Custom header in the error message to extract the originating partition from"
custom.offset.header.field.name="Custom header in the error message to extract the originating offset from"
error.handler.*="Define how to communicate to the cluster where the messages are. All communication properties a regular Kafka Producer/Consumer would need."
error.handler.producer.*="Allows to override the production communication settings, useful if the RESEND/REMAP operation should be to a different cluster."
```

# Try it out

The [docker-compose.yml](docker-compose.yml) that is included in this repository is based on the Confluent Platform Docker
images. It includes a single node Kafka Connect cluster that has been built with this connector for you to try out.

To run this, you will need docker running and docker compose installed. Clone this repo, and run:
```
docker-compose up -d
```