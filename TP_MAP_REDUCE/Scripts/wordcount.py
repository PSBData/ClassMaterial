import MapReduce
import sys

"""
Compte l'occurence de mots dans un corpus de texte

Entree : 

('ocid','Contenu du document')
('milton-paradise.txt',' Book I Of Man ' s first man... ')

Sortie :

('Book',1)
('man',2)
...

Tester le fichier sur books_sample_test.json puis Tweets_Ids_large.json

Pour executer le script :

	python chemin/vers/ce/script/wordcount.py chemin/vers/le/fichier/fichier.json

"""

mr = MapReduce.MapReduce()

# =============================
# Ne pas modifier le code au dessus

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, 1)

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
