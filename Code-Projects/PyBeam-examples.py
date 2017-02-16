# Standard imports
import apache_beam as beam
import re


"""Basic Pipeline"""
# Create a pipeline executing on a direct runner (local, non-cloud).
p = beam.Pipeline('DirectRunner')
# Create a PCollection with names and write it to a file.
(p
 | 'add names' >> beam.Create(['Ann', 'Joe'])
 | 'save' >> beam.io.WriteToText('./names'))
# Execute the pipeline.
p.run()


"""Basic pipeline (with Map)"""
p = beam.Pipeline('DirectRunner')
# Read a file containing names, add a greeting to each name, and write to a file.
(p
 | 'load names' >> beam.io.ReadFromText('./names*')
 | 'add greeting' >> beam.Map(lambda name, msg: '%s, %s!' % (msg, name), 'Hello')
 | 'save' >> beam.io.WriteToText('./greetings'))
p.run()



""" Counting words with GroupByKey , Flatmap"""
# This is a somewhat forced example of GroupByKey to count words as the previous example did, 
# but without using beam.combiners.Count.PerElement. As shown in the example,
#you can use a wildcard to specify the text file source.

class MyCountTransform(beam.PTransform):
  def expand(self, pcoll):
    return (pcoll
            | 'one word' >> beam.Map(lambda word: (word, 1))
            # GroupByKey accepts a PCollection of (word, 1) elements and
            # outputs a PCollection of (word, [1, 1, ...])
            | 'group words' >> beam.GroupByKey()
            | 'count words' >> beam.Map(lambda (word, counts): (word, len(counts))))

p = beam.Pipeline('DirectRunner')
(p
 | 'read' >> beam.io.ReadFromText('./names*')
 | 'split' >> beam.FlatMap(lambda x: re.findall(r'\w+', x))
 | MyCountTransform()
 | 'write' >> beam.io.WriteToText('./word_count'))
p.run()


"""Combiner Examples"""
#Combiner transforms use "reducing" functions, 
#such as sum, min, or max, to combine multiple values of a PCollection into a single value.
p = beam.Pipeline('DirectRunner')
SAMPLE_DATA = [('a', 1), ('b', 10), ('a', 2), ('a', 3), ('b', 20)]

(p
 | beam.Create(SAMPLE_DATA)
 | beam.CombinePerKey(sum)
 | beam.io.WriteToText('./sums'))
p.run()


"""Combiner with assert check"""
p = beam.Pipeline('DirectRunner')
SAMPLE_DATA = [('a', 1), ('b', 10), ('a', 2), ('a', 3), ('b', 20), ('c', 100)]
result = (p
     	| beam.Create(SAMPLE_DATA)
    	| beam.CombinePerKey(sum))
beam.assert_that(result, beam.equal_to([('a', 6), ('b', 30), ('c', 100)]))
p.run()









