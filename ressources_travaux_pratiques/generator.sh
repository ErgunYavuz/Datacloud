#!/bin/bash - 
#===============================================================================
#
#          FILE: generator.sh
# 
#         USAGE: ./generator.sh <src> <size-M>
# 
#        AUTHOR: Julien Sopena (julien.sopena@lip6.fr). 
#  ORGANIZATION: LIP6 - INRIA/CNRS
#       CREATED: 10/03/2013 23:08:55 CET
#      REVISION: 1.0
#===============================================================================

set -o nounset

[ $# -lt 2 ] && echo "Usage : $0 <fileName> <size>" && exit -1

src=$1
size=$2
fileName=$(basename $src)-$size
tmp=$src-tmp

[ -w $src ] || { echo "Impossible de lire $src" >&2; exit -1; }
[ $size -gt 0 ] 2>/dev/null || { echo "$size n'est pas un entier non nul" >&2; exit -1; }
cp $src $tmp || { echo "Impossible d'initialiser $tmp" >&2; exit -1; }

while [ "`du -BK $tmp | cut -f1 -dK`" -le 1024 ] ; do 
	cat $src >> $tmp
done

echo "" > $fileName || { echo "Impossible d'initialiser $fileName" >&2; exit -1; }

for i in $(seq 1 $size); do
	cat $tmp >> $fileName
done

rm $tmp || { echo "Impossible d'effacer le fichier $tmp" >&2; exit -1; }

