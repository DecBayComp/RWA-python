#!/bin/sh

versions="2.7 3.5 3.6 3.7 3.8 3.9 3.10 3.11"
#versions="3.6 2.7"

if [ "$(pwd | rev | cut -d/ -f1 | rev)" = "tests" ]; then
    container="$(pwd)/../containers/rwa-openmpi-dev.sif"
elif [ -d "tests" ]; then
    container="$(pwd)/containers/rwa-openmpi-dev.sif"
    cd tests
else
    echo "Please run $0 in the tests directory"
    exit 1
fi


if ! [ -f "$container" -o -h "$container" ]; then
    cd ../containers # if this crashes, $0 is not run from the tests directory as it should be
    echo "No container found; building one..."
    if [ -z "$(which apptainer)" ]; then
    echo "singularity build --fakeroot rwa-openmpi-dev.sif rwa-focal"
    singularity build --fakeroot rwa-openmpi-dev.sif rwa-focal || exit
    else
    echo "apptainer build rwa-openmpi-dev.sif rwa-jammy"
    apptainer build rwa-openmpi-dev.sif rwa-jammy || exit
    fi
    echo "======================================"
    echo "Container ready; starting the tests..."
    echo "======================================"
    cd ../tests
fi

hdf5_file=$(tempfile) || exit
poke_script=$(tempfile) || exit
peek_script=$(tempfile) || exit

trap "rm -f -- '$hdf_file' '$poke_script' '$peek_script'" EXIT

cat <<EOT > $poke_script
from rwa import *
from numpy.random import rand
from scipy.spatial import *
from pandas import Series
store = HDF5Store('$hdf5_file', 'w')#, verbose=True)

store.poke('range', range(4))
store.poke('delaunay', Delaunay(rand(5,2)))
store.poke('convhull', ConvexHull(rand(5,2)))
store.poke('voronoi', Voronoi(rand(5,2)))
store.poke('series', Series(range(4)))

store.close()
EOT

cat <<EOT > $peek_script
from rwa import *
store = HDF5Store('$hdf5_file', 'r', verbose=True)

store.peek('range')
store.peek('delaunay')
store.peek('convhull')
store.peek('voronoi')
store.peek('series')

store.close()
EOT


for poke_version in $versions; do

poke_python="singularity run $container -$(echo $poke_version | cut -c1,3-)"

for peek_version in $versions; do

if [ "$poke_version" = "$peek_version" ]; then
    continue
fi

peek_python="singularity run $container -$(echo $peek_version | cut -c1,3-)"

echo "----\n${poke_version}->${peek_version}\n----"

echo -n "poking with Python${poke_version}...\t"
$poke_python $poke_script && echo "[ok]"

echo -n "peeking with Python${peek_version}...\t"
$peek_python $peek_script && echo "[ok]"

done
done


rm -f -- '$hdf_file' '$poke_script' '$peek_script'
trap - EXIT
exit

