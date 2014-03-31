#!/bin/ksh -e
##########################################################################################
#
# Ported to RedHat by koaks, 20120821
#
##########################################################################################

OUTFILE=$1
MASTERLISTFILE=$2
RECORDTERM=$3

typeset -i i
i=0
f=""

(
while ((i<AB_PARTITION_INDEX))
do
	set +e
	read f
	rcode=$?
	set -e
	if [[ $rcode > 1 ]]
	then
		exit $rcode
	fi
	((i+=1))
done

i=$AB_NUMBER_OF_PARTITIONS-1

while read f appendstr
do
	((i+=1))
	if ((i==AB_NUMBER_OF_PARTITIONS)); then
		if [ "$RECORDTERM" ]; then
			gunzipper "$f" "$appendstr|" "$RECORDTERM"
		elif [ ${f##*.} = gz ]; then
			gzip -cd "$f"
      else
         cat "$f"
		fi
		i=0
	fi	
done
) < $MASTERLISTFILE > $OUTFILE
