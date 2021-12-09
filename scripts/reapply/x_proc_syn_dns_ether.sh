#!/bin/bash

PCAP=$1
F=$2
F2=$3

echo "Processing: $PCAP - $F - $F2"

echo "filter_v2"
/usr/sbin/tcpdump -n -i bond1 -tttt -vvv -r ${PCAP} tcp | python3 filter_v2.py ${F}
chown borescope.borescope filter_v2/${F2}
echo "done"

echo "filter_latlon"
/usr/sbin/tcpdump -n -i bond1 -tttt -vvv -r ${PCAP} tcp | python3 filter_latlon.py
chown borescope.borescope ${F2}
echo "done"

echo "non_client_ip_src"
python3 non_client_ip_src.py filter_v2/${F2} 71
chown borescope.borescope dynamic_ips/dynamic_ips.js
echo "done"

echo "dns_tinycubes"
/usr/sbin/tcpdump -n -i bond1 -tttt -vvv -r ${PCAP} udp port 53 | python3 dns_tinycubes.py
echo "done"

# echo "iteracoes_dns"
# /usr/sbin/tcpdump -n -i bond1 -tttt -vvv -r ${PCAP} | python3 iteracoes_dns_modv3.py
# echo "done"
