import MapReduce
import sys


"""
Ce script doit calculer identifier pour chaque mot une liste de document contenant ce mot, c'est 
une première étape vers un moteur de recherche

Entree : 

("tweetId1","ceci est mon premier tweet")
("tweetId2","C'est mon second tweet")
...

Sortie attendue :

("tweet",["tweetId1","TweetId2"])
("premier",["tweetId1"])
...

Indiquez à quoi correspondent les couples clés et valeurs en remplaçant les 
symboles _______ par du texte.

Ajuster le script puis tester le avec books_sample_test.json puis avec Tweets_Ids_large.json et Tweets_Dates_large.json

Pour executer le script :

	python chemin/vers/ce/script/Inverted_index.py chemin/vers/le/fichier/fichier.json

"""

mr = MapReduce.MapReduce()

# =============================
# Ne pas modifier le code au dessus

def mapper(record):
    # key: __________
    # value: __________
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, 1)

def reducer(key, list_of_values):
    # key: _______
    # value: __________
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

# Ne pas modifier le code ci-dessous
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
