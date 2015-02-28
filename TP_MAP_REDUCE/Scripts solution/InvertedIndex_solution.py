import MapReduce
import sys

"""
Count friends on a given social network
"""

mr = MapReduce.MapReduce()

# =============================
# Ne pas modifier le code au dessus

def mapper(record):
    # key: _________
    # value: ________
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w,key)

def reducer(key, list_of_values):
    # key: word
    # value: ________

    mr.emit((key,list_of_values))

# Ne pas modifier le code ci-dessous
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
