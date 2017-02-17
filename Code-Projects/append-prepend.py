import apache_beam as beam
#import re

p=beam.Pipeline('DirectRunner')

#append and prepend Pipeline - Xtrain
(p| 'load_input' >> beam.io.ReadFromText('./X_train')
 | 'append-prepend quotes' >> beam.Map(lambda x: "\""+x+"\"")
 |'write_' >> beam.io.WriteToText('./X_train_out') )
p.run()