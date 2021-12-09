#!/bin/bash

if [ $# != 1 ]
then
	echo "Eh necessario passar o nome do arquivo que sera gerado pelo tee"
	exit 1
fi

bash z_1.sh | tee $1
