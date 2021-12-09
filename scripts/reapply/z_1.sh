#!/bin/bash

TICKET_DIR="./tickets"
TICKET_IN_USE_DIR="./tickets_in_use"
SRC_DIR="./src"
DST_DIR="./dst"
SLEEP_TIME=15
NOW=`date +%Y-%m-%d_%H:%M:%S`

echo "-------------[ Starting: $NOW ]------------"
while true; do
	# pega ticket
	if [ ! -d ${TICKET_DIR} ] || [ ! -d ${SRC_DIR} ]; then
		# se nao tem
		echo "sleeping no ticket or source directory ..."
		sleep ${SLEEP_TIME}
	else 
		if [ -z "$(ls -A $TICKET_DIR)" ]; then
			# se nao tem
			echo "sleeping no ticket ..."
			sleep ${SLEEP_TIME}
		else
			if [ -z "$(ls -A $SRC_DIR/*.bz2)" ]; then
				# se nao tem
				echo "sleeping no source ..."
				#sleep ${SLEEP_TIME}
				exit 1
			else
				#echo "2"
				#exit
				ticket=`ls -1 $TICKET_DIR | head -n1`
				ticket2="$TICKET_DIR/$ticket"
				#ticket2_in_use="$TICKET_IN_USE_DIR/$ticket"
				
				source=`ls -1 $SRC_DIR | head -n1`
				source2="$SRC_DIR/$source"
				dst2="$DST_DIR/$source"
			
				#remove o ticket
				rm ${ticket2}
				
				#move para o destino
				mv ${source2} ${dst2}
				
				#echo "$source2 $dst2"
				echo "running z_2.sh $ticket2 $dst2"
				bash z_2.sh ${dst2} ${ticket2} &
				#nice -n 10 bash z_2.sh ${dst2} ${ticket2} &
				#nice -n 10 bash z_2.sh ${dst2} ${ticket2} ${ticket2_in_use} &
				
			fi
		fi		
	fi
done	   