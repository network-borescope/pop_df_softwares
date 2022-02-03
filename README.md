# Softwares utilizados no PoP

## pcap_to_ml
Este script em Python implementa um processador de tráfego coletado que relaciona conversas(request-response) DNS com pedidos de conexão Web por cliente. Estas informações alimentam algoritmos de ML(Machine Learning) que são utilizados para encontrar anomalias na rede. Vale ressaltar que o IP que realiza o pedido de conexão não é obrigatoriamente o mesmo que fez a requisição DNS, mas ambos devem pertencer ao mesmo cliente.

O script é executado utilizando "pipe" na saída do TCPDump ao ler um arquivo PCAP.

``tcpdump -n -i bond1 -tttt -vvv -r ${PCAP} | python3 iteracoes_dns_modv3.py``

O arquivo de saída do processador, está no padrão aceito pelo Tinycubes e possui formatação de acordo com o exemplo a seguir.

2021-07-03,7,16,55,200.130.17.1,4,124,8.8.8.8,27005,1,0,0,0,0,1,200.130.17.1,4,60,443,HTTP,161.148.164.31,www.gov.br,0:00:00.062746

1) data no formato YYYY-MM-DD: 2021-07-03
2) dia da semana: 7
3) hora: 16
4) id do client: 55
5) ip de origem da requisição DNS: 200.130.17.1
6) distância da requisição DNS: 4
7) TTL da requisição DNS: 124
8) ip de destino da requisição DNS. O valor é 0 se não for um servidor open DNS: 8.8.8.8
9) ip-id da requisição DNS: 27005
10) flag '+' da conversa DNS(requisição): 1
11) flag '%' da conversa DNS(requisição): 0
12) flag '*' da conversa DNS(resposta): 0
13) flag '-' da conversa DNS(resposta): 0
14) flag '|' da conversa DNS(resposta): 0
15) flag '$' da conversa DNS(resposta): 1
16) ip de origem da requisição Web: 200.130.17.1
17) distância da requisição Web: 4
18) TTL da requisição Web: 60
19) porta de destino da requisição Web: 443
20) serviço: HTTP
21) ip de destino da requisição Web: 161.148.164.31
22) query feita pela requisição DNS e acessada pela requisição Web: www.gov.br
23) Deltatime, tempo decorrido entre a resposta DNS e o acesso Web: 0:00:00.062746

## pcap_to_dns
Este script em Python implementa um processador de tráfego coletado que gera informações estatísticas a respeito do tráfego DNS por cliente. Dentre as informações produzidas estão, a quantidade de requisições DNS feitas e recebidas pelo cliente e a quantidade de respostas DNS recebidas e enviadas pelo cliente. A partir destas informações pode-se verificar a eficiência da comunicação DNS e também inferir se o cliente está sofrendo ou realizando ataques.

O script é executado utilizando "pipe" na saída do TCPDump ao ler um arquivo PCAP.

``tcpdump -n -i bond1 -tttt -vvv -r ${PCAP} udp port 53 | python3 dns_tinycubes.py``

O arquivo de saída do processador, está no padrão aceito pelo Tinycubes e possui formatação de acordo com o exemplo a seguir.

2021-08-24 19:17:00;-15.816786;-47.943379;1;55;123;5;1;470;340;1;1;184;161;200.130.9.199;200.130.9.7

1) Data-hora (resolução de minuto): 2021-08-24 19:17:00
2) Latitude: -15.816786
3) Longitude: -47.943379
4) Id do PoP(sempre 1): 1
5) Id do Cliente: 55
6) TTL do pior ip de origem: 123
7) Total de requisições feitas pelo cliente: 5
8) Total de requisições feitas pelo cliente que não foram respondidas: 1
9) Total de requisições recebidas pelo Cliente: 470
10) Total de requisições recebidas pelo Cliente que não foram respondidas: 340
11) Total de requisições feitas pela pior origem: 1
12) Total de requisições feitas pela pior origem que não foram respondidas: 1
13) Total de requisições recebidas pelo pior destino: 184
14) Total de requisições recebidas pelo pior destino que não foram respondidas: 161
15) Pior Origem: 200.130.9.199
16) Pior Destino: 200.130.9.7

OBS:

**Pior Origem**: Ip daquele cliente que, atuando como origem tem o maior percentual de requisições não atendidas. O critério de desempate é a quantidade absoluta de requisições

**Pior Destino**: Ip daquele cliente que, atuando como destino tem o maior percentual de requisições não atendidas. O critério de desempate é a quantidade absoluta de requisições.
## pcap_to_tc_serv
Este script em Python implementa um processador de tráfego coletado que gera uma contagem dos pacotes de cada serviço utlizado pelo cliente a partir do tráfego TCP. A partir destas informações pode-se verificar a distribuição de serviços utilizados pelo cliente e posteriormente traçar um perfil de utilização por cliente.

O script é executado utilizando "pipe" na saída do TCPDump ao ler um arquivo PCAP.

``tcpdump -n -i bond1 -tttt -vvv -r ${PCAP} tcp | python3 filter_latlon.py``

O arquivo de saída do processador, está no padrão aceito pelo Tinycubes e possui formatação de acordo com o exemplo a seguir.

2021-08-24 18:58:00;-15.795423;-47.873152;1;58;59;5;1;18;2

1) data-hora (resolução de minuto): 2021-08-24 18:58:00
2) lat: -15.795423
3) lon: -47.873152
4) id_pop (sempre 1): 1
5) id_cliente: 58
6) ttl: 59
7) distância: 5
8) tp_servico(código do serviço): 1
9) id_destino: 18
10) quantidade_de_pacotes: 2

### Codificação dos Serviços
| Porta | Serviço | Código |
|-------|---------|--------|
|  80   |   Web   |    1   |
|  443  |   Web   |    1   |
|  25   |   SMTP  |    2   |
|  465  |   SMTP  |    2   |
|  587  |   SMTP  |    2   |
|  110  |   POP3  |    3   |
|  995  |   POP3  |    3   |
|  143  |   IMAP  |    4   |
|  20   |   FTP   |    5   |
|  21   |   FTP   |    5   |
|  22   |   SSH   |    6   |
|  53   |DNS(TCP) |    7   |

OBS:
- **Web**: HTTP(porta 80) e HTTPS(porta 443).
- **SMTP**: Simple Mail Transfer Protocol.
- **POP3**: Post Office Protocol version 3.
- **IMAP**: Internet Message Access Protocol.
- **FTP**: File Transfer Protocol.
- **SSH**: Secure Shell.
## filter_c
Este programa desenvolvido em C roda continuamente e tem como objetivo filtrar o tráfego saindo da rede do PoP. A filtragem consiste em contabilizar a ocorrência das tuplas formadas a partir de informações extraídas dos pacotes.

O programa é executado utilizando o script abaixo.

```Shell
#!/bin/bash
internal_ether="cc:4e:24:42:55:0d"
nohup $SHELL <<EOF > /dev/null &
sudo tcpdump -l -U -vvv -n -tttt -i bond1 ether src ${internal_ether}  "$@"  | ./filter_c
EOF
```

O programa gera arquivos, a cada minuto, no padrão aceito pelo Tinycubes e possui formatação de acordo com o exemplo a seguir.

2021-12-31 11:11:00;-15.795423;-47.873152;1;58;60;6;0;0;1050

1) data no formato YYYY-MM-DD hh:mm:00: 2021-12-31 11:11:00
2) lat: -15.795423
3) lon: -47.873152
4) id do pop(sempre 1): 1
5) id_cliente: 58
6) ttl: 60
7) protocolo: 6
8) port_dst: 0
9) 0: 0
10) count: 1050

OBS: A tupla que é contabilizada é composta pelas informações 1 a 9.
## scripts
