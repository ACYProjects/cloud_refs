from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apeche_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apeche_beam.transforms import Map, Filter, WindowInto, CombinePerKey

project_id = 'project_id'
pubsub_topic = '/projects/'+ project_id + '/topics/topic_name'
dataset = 'dataset'
table = 'table'

schema = schema = "field:STRING, field2:FLOAT64, field3:TIMESTAMP"
window_duration = 60 * 5

def get_timestamp(message):
  return data_element['timestamp']

def data_validation(message):
  if len(data['field1'] > 0):
    return message
  else:
    None

def filter_data(message):
  if data_validation(mesaage):
    return message
  else:
    None

options = PipelineOptions(
    project= project_id,
    runner= "DataflowRunner"
    region= "us-central1"
)

with beam.Pipeline(options=options) as p:
  data = "ReadFromPubSub" >> ReadFromPubsub(topic=topic_name)
  data = "Validation" >> Map(validate)
  data = 'Filter_Data' >> Filter(Filter_data)

  data = "Get_timestamp" >> Map(get_timestamp)
  data = "Window_data" >> WindowInto(window_duration=window_duration)

  data = 'WriteToBigQuery' >> WriteToBigQuery(
      table= f'{project_id}.{dataset}.{table}',
      schema=schema,
      create_disposition='CREATE_IF_NEDEED',
      write_disposition='WRITE_APPEND'
  )


if __name__='__main__':
  p.run()
