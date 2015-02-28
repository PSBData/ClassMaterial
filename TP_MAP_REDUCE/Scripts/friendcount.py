import MapReduce
import sys

"""
Ce script doit calculer le nombre d'amis à partir d'une liste de connections

Entrée : 

("profil1","profil2")
("profil2","profil3")
...

Sortie attendue :

("profil1",1)
("profil2",2)
("profil3",1)
...

Indiquez à quoi correspondent les couples clés et valeurs en remplaçant les 
symboles _______ par du texte.

Ajuster le script tel que vu en cours!

Testez le script sur le fichier  friends_sample_test.json puis Twitter_friends_library_large.json

Pour executer le script :

	python chemin/vers/ce/script/friendcount.py chemin/vers/le/fichier/fichier.json

"""

mr = MapReduce.MapReduce()

# =============================
# Ne pas modifier le code au dessus

def mapper(record):
    # key:   __________
    # value: __________
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, 1)

def reducer(key, list_of_values):
    # key:   __________
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
