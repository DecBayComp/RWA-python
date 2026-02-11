#!/bin/sh

set -e

versions="2.7 3.5 3.6 3.7 3.8 3.9 3.10 3.11 3.12 3.13 3.14"

if [ -f "tests/multi-py_compat.sh" ]; then
  cd tests
elif ! [ -f "multi-py_compat.sh" ]; then
  echo "Please run $0 in the tests directory" >&2
  exit 1
fi
# clean up after failed test runs
rm -rf tmp.*

if command -v podman >/dev/null; then
  if [ -z "`podman images | grep localhost/rwa-python`" ]; then
    podman build -t rwa-python -f ../containers/Containerfile ..
  fi
  run() {
    local ver=$1 path=$2
    shift 2
    local file=`basename "$path"` dir=`dirname "$path"`
    podman run -v "$dir":/data --security-opt label=disable --rm rwa-python "$ver" "/data/$file" "$@"
  }
elif command -v apptainer >/dev/null; then
  container=`realpath ../containers/rwa-jammy.sif`
  if [ ! -f "$container" ] && [ ! -h "$container" ]; then
    (cd ../containers; apptainer build rwa-jammy.sif rwa-jammy)
  fi
  run() {
    apptainer run $container "$@"
  }
else
  echo "No container engines found" >&2
  exit 1
fi

tmpdir=$(mktemp -d -p .) || exit
hdf5_file=$(mktemp -p $tmpdir) || exit
poke_script=$(mktemp -p $tmpdir) || exit
peek_script=$(mktemp -p $tmpdir) || exit

trap "rm -f -- '$hdf_file' '$poke_script' '$peek_script'" EXIT

cat <<EOT > $poke_script
import os
file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '`basename "$hdf5_file"`')

from rwa import *
from numpy.random import rand
from scipy.spatial import *
from pandas import Series
store = HDF5Store(file, 'w')#, verbose=True)

store.poke('range', range(4))
store.poke('delaunay', Delaunay(rand(5,2)))
store.poke('convhull', ConvexHull(rand(5,2)))
store.poke('voronoi', Voronoi(rand(5,2)))
store.poke('series', Series(range(4)))

store.close()
EOT

cat <<EOT > $peek_script
import os
file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '`basename "$hdf5_file"`')

from rwa import *
store = HDF5Store(file, 'r', verbose=True)

store.peek('range')
store.peek('delaunay')
store.peek('convhull')
store.peek('voronoi')
store.peek('series')

store.close()
EOT


i=0
for poke_version in $versions; do

i=$(( i + 1 ))

poke_python="-$(echo $poke_version | cut -c1,3-)"

j=0
for peek_version in $versions; do

j=$(( j + 1 ))
[ $i -le $j ] || continue

if [ "$poke_version" = "$peek_version" ]; then
  continue
fi

peek_python="-$(echo $peek_version | cut -c1,3-)"

echo -e "----\n${poke_version}->${peek_version}\n----"

echo -ne "poking with Python${poke_version}...\t"
run $poke_python $poke_script && echo "[ok]"

echo -ne "peeking with Python${peek_version}...\t"
run $peek_python $peek_script && echo "[ok]"

done
done


rm -rf -- '$tmpdir'
trap - EXIT
exit

