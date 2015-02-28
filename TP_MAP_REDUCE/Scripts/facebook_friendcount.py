import MapReduce
import sys


"""
Ce script doit calculer le nombre d'amis à partir d'une liste de connections

Entrée : 

("profil1","profil2,profil3,profil4,profil5")
("profil2","profil3,profil10,profil15")
...

Sortie attendue :

("profil1",1)
("profil2",3)
("profil3",2)
("profil10,1)
...

Indiquez à quoi correspondent les couples clés et valeurs en remplaçant les 
symboles _______ par du texte.

Ajuster le script puis tester le avec facebook_friends_large.json

Pour executer le script :

	python chemin/vers/ce/script/facebook_friendcount.py chemin/vers/le/fichier/fichier.json

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
