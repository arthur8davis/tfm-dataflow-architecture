import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    options = PipelineOptions([
        '--runner=DirectRunner',
        '--direct_num_workers=1',
        '--direct_running_mode=multi_threading',
    ])

    with beam.Pipeline(options=options) as p:
        (p 
         | 'Create' >> beam.Create(['Hello', 'World', 'Flink'])
         | 'Print' >> beam.Map(print)
        )

if __name__ == '__main__':
    run()