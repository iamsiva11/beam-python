"""
Refer:
https://medium.com/google-cloud/quickly-experiment-with-dataflow-3d5a0da8d8e9#.x6r4iwjoj
"""

import apache_beam as beam


['a','b'] | beam.Map(lambda x: (x, 1))

[(‘Jan’,3), (‘Jan’,8), (‘Feb’,12)] | beam.GroupByKey()

[('Jan',3), ('Jan',8), ('Feb',12)] | \
beam.GroupByKey() | \
beam.Map(lambda (mon,days) : (mon,len(days)))