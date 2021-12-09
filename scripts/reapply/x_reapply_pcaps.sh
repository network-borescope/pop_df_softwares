#!/bin/bash

#
# Parametro: arquivo contendo uma lista de nomes-de-arquivo do tipo PCAP.BZ2, presentes no diretorio pcap, a serem processados
# DICA: O arquivo de PCAPS.TXT pode ser gerado usando "ls -1 pcap/" e depois editado
#

if [ "$1" eq "" ]; then
	echo "Missing first argument: text filename containing a list of pcap files to reprocess"
	exit
fi


PCAPS=$(cat "$1")
for FILE in $PCAPS
do
	PCAP_BZ2=pcap/$FILE
	F=${FILE%.bz2}
	PCAP=pcap/${F}
	F2="ips_${F%.pcap}.txt"

	echo "-------------"
	echo $PCAP
	echo $F
	echo $F2

	bzip2 -dk $PCAP_BZ2
    bash x_proc_syn_dns_ether.sh $PCAP $F $F2           
	# $sudo tcpdump -n -i bond1 -tttt -vvv -r ${filename%.bz2} | python3 iteracoes_dns_modv3.py
    rm $PCAP
done

