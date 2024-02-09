import argparse
import logging
import argparse, logging, os
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from signal import signal, SIGPIPE, SIG_DFL 

signal(SIGPIPE,SIG_DFL)

class ReadGBK(beam.DoFn):
 def process(self, e):
   k, elems = e
   for v in elems:
     logging.info(f"the element is {v}")
     yield v
def run(argv=None):
   parser = argparse.ArgumentParser()
   parser.add_argument(
     '--output', dest='output', help='Output file to write results to.')
   known_args, pipeline_args = parser.parse_known_args(argv)
   read_query = """(
                 SELECT
                   version,
                   block_hash,
                   block_number
                 FROM
                   `bigquery-public-data.crypto_bitcoin.transactions`
                 WHERE
                   version = 1
                 LIMIT
                   1000 )
               UNION ALL (
                 SELECT
                   version,
                   block_hash,
                   block_number
                 FROM
                   `bigquery-public-data.crypto_bitcoin.transactions`
                 WHERE
                   version = 2
                 LIMIT
                   10 ) ;"""
   p = beam.Pipeline(options=PipelineOptions(pipeline_args))
   (p
   | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=read_query, use_standard_sql=True)
   | "Add Hotkey" >> beam.Map(lambda elem: (elem["version"], elem))
   | "Groupby" >> beam.GroupByKey()
   | 'Print' >>  beam.ParDo(ReadGBK())
   | 'Sink' >>  WriteToText(known_args.output))
   result = p.run()
if __name__ == '__main__':
    try:
        logger = logging.getLogger().setLevel(logging.INFO)
        run()
    except IOError as e:
        if e.errno == errno.EPIPE:
            pass
    
    
 
 