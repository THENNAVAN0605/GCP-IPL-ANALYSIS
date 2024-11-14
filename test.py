import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class FilterOutLowestWicketTakers(beam.DoFn):
    def process(self, element):
        over = element['over']
        player_out = element['player_out']
        if player_out:
            yield (player_out, over)

def run():
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(
                query='SELECT * FROM `project-id.dataset.deliveries`',
                use_standard_sql=True)
            | 'Extract player and over' >> beam.ParDo(FilterOutLowestWicketTakers())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                'project-id.dataset.wicket_analysis',
                schema='player_out:STRING, over:INTEGER')
        )

if __name__ == '__main__':
    run()
