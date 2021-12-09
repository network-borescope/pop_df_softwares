#!/bin/bash

#
# Roda as conversoes em modo de REAPLICACAO
#

if [ "$#" != "2" ]; then
	echo "Missing argument2."
	echo "Usage: bash THIS_SCRIPT <pcap_bz2_filename> <ticket_filename>"
#	echo "Usage: bash THIS_SCRIPT <pcap_bz2_filename> <ticket_filename> <ticket_in_use_filename>"
	exit
fi

# move o ticket para ticket_in_use
#echo "move $2 -> $3"
#mv $2 $3
				
FULL_FILENAME=$1
#FULL_FILENAME="111/222/333/444"
#FULL_FILENAME="444"
PATH_NO_SLASH=${FULL_FILENAME%/*}
FILENAME=${FULL_FILENAME#"$PATH_NO_SLASH/"}
#echo $FILENAME

PCAP_BZ2=$FULL_FILENAME
F=${FILENAME%.bz2}
PCAP="$PATH_NO_SLASH/$F"

	
F2="ips_${F%.pcap}.txt"

echo "-------------"
echo $PCAP
echo $F
echo $F2

bzip2 -dk $PCAP_BZ2
bash x_proc_syn_dns_ether.sh $PCAP $F $F2           
	# $sudo tcpdump -n -i bond1 -tttt -vvv -r ${filename%.bz2} | python3 iteracoes_dns_modv3.py
rm $PCAP

# recria o ticket
touch $2

# devolve o ticket
#mv $3 $2
				
