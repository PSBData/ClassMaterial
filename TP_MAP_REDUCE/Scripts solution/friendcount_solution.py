import MapReduce
import sys

"""
Count friends on a given social network
"""

mr = MapReduce.MapReduce()

# =============================
# Ne pas modifier le code au dessus

def mapper(record):
    # key: 
    # value: 
    key = record[0]
    value = record[1]
    
    mr.emit_intermediate(key, 1)
    mr.emit_intermediate(value,1)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

# Ne pas modifier le code ci-dessous
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
