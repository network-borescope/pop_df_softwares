#!/bin/bash
#
# Script coletor de dados relevantes do POP
#
# Coleta T segundos (normalmente 360) de dados em determinados minutos de cada hora utilizando o cron.
#
# Como a inicialização do processo de coleta pode demorar alguns segundos, para que não haja perdas, 
# este script é iniciado 1 minuto antes do momento desejado para coleta e "dorme" por 50 segundos 
# imediatamente antes de comecar a coleta. Como resultado, a coleta se inicia quase 10 segundos antes 
# do desejado. Ao mesmo tempo, como o término do processo de coleta também não é imediato, costuma 
# ocorrer um excesso de coleta de alguns milisegundos. Esse excesso causa um certo ruído nas análises echo
# deve ser removido.
# Por conta dos excessos acima, após a coleta dos eventos num arquivo pcap, esse arquivo resultante é 
# encurtado retirando os excessos. Isso é realizado pela ferramenta "editcap".
#


cd /home/borescope/run0/test1
T=360
# como a coleta so comecara efetivamente daqui a um minuto, devemos adicionar esse minuto a todos
D=`date +"%Y-%m-%d-%H-%M" -d "+1 minute"`
DB=`date +"%Y-%m-%d %H:%M:00" -d "+1 minute +$T seconds"`
DA=`date +"%Y-%m-%d %H:%M:00" -d "+1 minute"`
echo "DA = $DA" "DB = $DB"

F=syn_dns_ether_${T}s_${D}.pcap
TEMP=pcap/"temp_$F"
PCAP=pcap/${F}
F2=out_syn_dns_ether_${T}s_${D}.txt
internal_ether="cc:4e:24:42:55:0d"

sleep 50 # mimir por 50 segundos
T=$((T+10))
sudo timeout ${T} tcpdump -n -i bond1 -tttt -vvv not net 10.0.0.0/8 and "(tcp[tcpflags] == tcp-syn and ether src $internal_ether) or udp port 53" -w ${TEMP}
editcap -A "$DA" -B "$DB" ${TEMP} ${PCAP}
chown borescope.borescope ${PCAP}
rm ${TEMP}

#x_proc_sys_dns_ehter.sh $PCAP $F $F2

bzip2 ${PCAP}

#bash rsync_to_server.sh

