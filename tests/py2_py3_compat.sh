#!/bin/sh

t=$(tempfile) || exit
trap "rm -f -- '$t'" EXIT

poke="from rwa import *; from numpy.random import rand; from scipy.spatial import Delaunay; from pandas import Series; store = HDF5Store('$t','w'); store.poke('delaunay',Delaunay(rand(5,2))); store.poke('series',Series(range(4))); store.close()"

peek="from rwa import *; store = HDF5Store('$t','r'); store.peek('series'); store.peek('delaunay'); store.close()"

echo "----\n2->3\n----"
echo -n "poking with Python2...\t"
python2 -c "${poke}" && echo "[ok]"
echo -n "peeking with Python3...\t"
python3 -c "${peek}" && echo "[ok]"

echo "----\n3->2\n----"
echo -n "poking with Python3...\t"
python3 -c "${poke}" && echo "[ok]"
echo -n "peeking with Python2...\t"
python2 -c "${peek}" && echo "[ok]"

rm -f -- "$t"
trap - EXIT
exit

