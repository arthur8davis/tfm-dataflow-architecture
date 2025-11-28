import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka

def decode(kv):
    key, value = kv
    if value is not None:
        print(">> MENSAJE LEÍDO:", value.decode("utf-8"))
    else:
        print(">> MENSAJE SIN VALOR")
    return kv

def run():
    options = PipelineOptions(
        streaming=True,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromKafka" >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": "localhost:9092",
                    "group.id": "beam-test",
                    "auto.offset.reset": "earliest"
                },
                topics=["cases"]
            )
            | "Decode" >> beam.Map(decode)
        )

if __name__ == "__main__":
    run()