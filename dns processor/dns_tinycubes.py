"""

Programa que gera arquivos .csv no padrão tinycubes a partir de dados coletados no pop df

Le os pacotes DNS e forma pares (request, response) com o objetivo de fazer uma totalizacao por CLIENTE, gerando a linha:

2021-08-24 19:17:00;-15.816786;-47.943379;1;55;123;5;1;470;340;1;1;184;161;200.130.9.199;200.130.9.7

1) Data-hora (resolução de minuto): 2021-08-24 19:17:00
2) Latitude: -15.816786
3) Longitude: -47.943379
4) Id do PoP(sempre 1): 1
5) Id do Cliente: 55
6) TTL do pior ip de origem: 123
7) Total de requisicoes feitas pelo cliente: 5
8) Total de requisicoes feitas pelo cliente que nao foram respondidas: 1
9) Total de requisicoes recebidas pelo Cliente: 470
10) Total de requisicoes recebidas pelo Cliente que nao foram respondidas: 340
11) Total de requisicoes feitas pela pior origem: 1
12) Total de requisicoes feitas pela pior origem que nao foram respondidas: 1
13) Total de requisicoes recebidas pelo pior destino: 184
14) Total de requisicoes recebidas pelo pior destino que nao foram respondidas: 161
15) Pior Origem: 200.130.9.199
16) Pior Destino: 200.130.9.7

OBS:
**Pior Origem: Ip daquele cliente que, atuando como origem tem o maior percentual de requisicoes nao atendidas. O criterio de desempate e a qtd absoluta de requisicoes.
**Pior Destino: Ip daquele cliente que, atuando como destino tem o maior percentual de requisicoes nao atendidas. O criterio de desempate e a qtd absoluta de requisicoes.


As linhas são grupadas em arquivos a cada minuto.
 
"""
from sys import argv, exit, stdin
import os
import datetime
from ip_to_nome_lat_lon import site_from_ip

# globals and functions

#VERSION = "2.0"
DATA_HORA_FILE = None # yyyymmdd_hh_nn
DATA_HORA_BASE = None
MINUTO_ATUAL = None
MINUTO_MAX = 1 # faz flush se: minuto_processado - MINUTO_ATUAL >= MINUTO_MAX
LAST_TIMEDELTA = None

def hour_to_timedelta(d_hora):
    hour, min, sec = d_hora.split(":")
    
    return datetime.timedelta(hours=int(hour), minutes=int(min), seconds=float(sec))

def dns_req_eof(last_timedelta, current_timedelta, delta_seconds=30):
    delta = datetime.timedelta(seconds=delta_seconds)
    
    if current_timedelta <= last_timedelta - delta: return False
    
    return True

def is_client(ip):
    result = site_from_ip(ip)

    if result[6] == "1": return ip
    return ip + " (" + result[0] + ")" # return client_name

def get_query(items):
    n = len(items)
    i = 7 # indice inicial
    
    query_type = None
    query = None
    while i < n:
        if len(items[i]) > 0 and items[i][-1] == "?": # query type
            query_type = items[i]

            i += 1
            if not i < n: return None

            if len(items[i]) > 0 and items[i][-1] == ".": # query termina com "."
                query = items[i][:-1] # remove o ponto

                return query
            
            return None
        
        i += 1
    
    return None

def dict_insert(d, key, ip, ttl = None):
    d[key] = {
        "client_as_source": {
            "total_req": 0,
            "total_pairs": 0,
            "total_req_sem_resp": 0,
            "total_req_repetida": 0
        },
        "client_as_target": {
            "total_req": 0,
            "total_pairs": 0,
            "total_req_sem_resp": 0,
            "total_req_repetida": 0
        }
    }
    dict_insert_ip(d, key, ip, ttl)

def dict_insert_ip(d, key, ip, ttl = None):
    if ttl is not None:
        d[key][ip + "-" + ttl] = {
            "type": "source",
            "total_req": 0,
            "total_pairs": 0,
            "total_req_sem_resp": 0,
            "total_req_repetida": 0
        }
    else:
        d[key][ip] = {
            "type": "target",
            "total_req": 0,
            "total_pairs": 0,
            "total_req_sem_resp": 0,
            "total_req_repetida": 0
        }

def dict_flush(d:dict, dns_match:dict):
    for key,dns in dns_match.items():
        if dns_req_eof(LAST_TIMEDELTA, dns[REQUEST_TIME], delta_seconds=5) and dns[RESPONSE] is None: continue

        src_key = dns[KEY_SRC]
        target_key = dns[KEY_DST]

        if src_key is not None:
            d[src_key]["client_as_source"]["total_req"] += 1
            d[src_key][dns[IP_SRC]+"-"+dns[TTL_SRC]]["total_req"] += 1

        if target_key is not None:
            d[target_key]["client_as_target"]["total_req"] += 1
            d[target_key][dns[IP_DST]]["total_req"] += 1

        if dns[RESPONSE] is not None:
            if src_key is not None:
                d[src_key]["client_as_source"]["total_pairs"] += 1
                d[src_key][dns[IP_SRC]+"-"+dns[TTL_SRC]]["total_pairs"] += 1
            
            if target_key is not None:
                d[target_key]["client_as_target"]["total_pairs"] += 1
                d[target_key][dns[IP_DST]]["total_pairs"] += 1


    #filename = "v_" + VERSION + "_dns_" + DATA_HORA_FILE + ".csv"
    filename = "csv_dns/" + DATA_HORA_FILE + ".csv"
    print("Gerando", filename)
    with open(filename, "w") as fout:
        os.system("chown borescope.borescope " + filename)
        for key in d:
            client_total_req_as_source = d[key]["client_as_source"]["total_req"]
            client_total_req_sem_resp_as_source = d[key]["client_as_source"]["total_req"] - d[key]["client_as_source"]["total_pairs"]

            client_total_req_as_target = d[key]["client_as_target"]["total_req"]
            client_total_req_sem_resp_as_target = d[key]["client_as_target"]["total_req"] - d[key]["client_as_target"]["total_pairs"]

            del d[key]["client_as_source"]
            del d[key]["client_as_target"]

            # encontrados os piores source e target
            pior_percent_as_source = -1.0 # percentual de req nao atendidas
            pior_percent_as_target = -1.0 # percentual de req nao atendidas

            pior_source_qtd = 0
            pior_target_qtd = 0

            pior_source = None # ip do pior
            pior_target = None # ip do pior

            for ip in d[key]: # no caso do source, sera ip-ttl
                # source
                if d[key][ip]["type"] == "source":
                    if d[key][ip]["total_req"] > 0:
                        req_sem_resp = d[key][ip]["total_req"] - d[key][ip]["total_pairs"]
                        percent = (req_sem_resp/d[key][ip]["total_req"]) * 100 # percentual de req nao atendidas
                        d[key][ip]["total_req_sem_resp"] = req_sem_resp # salva valor

                        if percent > pior_percent_as_source or (percent == pior_percent_as_source and req_sem_resp > pior_source_qtd):
                            pior_percent_as_source = percent
                            pior_source = ip
                            pior_source_qtd = req_sem_resp
                    else:
                        #print("Nao eh source de req DNS: ", key, ip)
                        pass

                # as target
                if d[key][ip]["type"] == "target":
                    if d[key][ip]["total_req"] > 0:
                        req_sem_resp = d[key][ip]["total_req"] - d[key][ip]["total_pairs"]
                        percent = (req_sem_resp/d[key][ip]["total_req"]) * 100 # percentual de req nao atendidas
                        d[key][ip]["total_req_sem_resp"] = req_sem_resp # salva valor

                        if percent > pior_percent_as_target or (percent == pior_percent_as_target and req_sem_resp > pior_target_qtd):
                            pior_percent_as_target = percent
                            pior_target = ip
                            pior_target_qtd = req_sem_resp
                    else:
                        #print("Nao eh target de req DNS: ",key, ip)
                        pass
                
            # pega os valores dos piores
            ttl_src = "0"
            if pior_source is not None:
                total_req_pior_source = d[key][pior_source]["total_req"]
                total_req_sem_resp_pior_source = d[key][pior_source]["total_req_sem_resp"]
                pior_source, ttl_src = pior_source.split("-")
            else:
                total_req_pior_source = 0
                total_req_sem_resp_pior_source = 0
                pior_source = "0"

            if pior_target is not None:
                total_req_pior_target = d[key][pior_target]["total_req"]
                total_req_sem_resp_pior_target = d[key][pior_target]["total_req_sem_resp"]
            else:
                total_req_pior_target = 0
                total_req_sem_resp_pior_target = 0
                pior_target = "0"
            
            # constroi linha
            line = key + ";"
            line += ttl_src + ";"
            line += str(client_total_req_as_source) + ";"
            line += str(client_total_req_sem_resp_as_source) + ";"
            line += str(client_total_req_as_target) + ";"
            line += str(client_total_req_sem_resp_as_target) + ";"
            line += str(total_req_pior_source) + ";"
            line += str(total_req_sem_resp_pior_source) + ";"
            line += str(total_req_pior_target) + ";"
            line += str(total_req_sem_resp_pior_target) + ";"
            line += pior_source + ";"
            line += pior_target

            print(line, file=fout) # escreve no arquivo

def must_flush(d_hora):
    minute = int(d_hora.split(":")[1])

    return minute - MINUTO_ATUAL >= MINUTO_MAX

def dns_match_insert(dns_match:dict, dns_key, request_time, ttl_src, key_src, key_dst, ip_src, ip_dst):
    dns_match[dns_key] = [1, None, request_time, ttl_src, key_src, key_dst, ip_src, ip_dst]

    return

# Main

arguments = argv[1:]
if len(arguments) == 1:
    filename = arguments[0]
    fin = open(filename, "r")
elif len(arguments) == 0:
    fin = stdin
else:
    exit(1)

try:
    os.mkdir("csv_dns")
except OSError: pass

tinycubes_dict = {}

dns_match = {}
REQUEST = 0
RESPONSE = 1
REQUEST_TIME = 2
TTL_SRC = 3
KEY_SRC = 4
KEY_DST = 5
IP_SRC = 6
IP_DST = 7
#QTD_REPT = 8 # quantas vezes o request se repetiu

data = []
# data positions
D_DATA = 0
D_HORA = 1
D_TTL = 2
D_PROTO = 3
D_IP_ID = 4
D_SIP= 5
D_SPORT = 6
D_DIP= 7
D_IDC = 8
D_QUERY = 9
D_HOST = 10
D_US_AG = 11
D_DIST = 12
D_IDD = 13

#count = 0

for line in fin:
    #count+=1
    #if count % 1000000 == 0: print(count)

    ident = 0
    altura = 0
    if len(line) == 0: continue

    # verifica se eh a linha de header
    while line[0] == ' ' or line[0] == '\t':
        if line[0] == '\t': ident += 8
        else: ident+=1
        line =line[1:]

    altura = ident/4

    if altura == 0:
        # reinicia as variaveis de memoria
        key = ""
        data = []

        # inicia o processamento do novo hash
        clean_line = line.strip()
        items = clean_line.split(" ")

        if len(items) < 6: continue
        if items[2] != "IP": continue

        n = len(items)

        # [data, hora, val_ttl, val_proto, val_ip_id ]
        val_proto = items[15][1:-2]
        data = [ items[0], items[1], items[6].strip(","), val_proto, items[8].strip(","), "0", "0", "0", "0", "0", "0", "0", "0", "0" ]

        if data[D_TTL] == "oui":
             data = []
             continue

        dist = int(data[D_TTL])
        if dist < 64: dist = 64 - dist
        elif dist < 128: dist = 128 - dist
        else: dist = 255 - dist
        data[D_DIST] = str(dist)

        if DATA_HORA_FILE is not None and must_flush(data[D_HORA]):
            dict_flush(tinycubes_dict, dns_match)
            dns_match = {}
            tinycubes_dict = {}

            MINUTO_ATUAL = None
            DATA_HORA_BASE = None
            DATA_HORA_FILE = None

        if DATA_HORA_FILE is None:
            year, month, day = data[D_DATA].split("-")
            hour, minute = data[D_HORA].split(":")[:-1]
            DATA_HORA_FILE = year + month + day + "_" + hour + "_" + minute

            DATA_HORA_BASE = data[D_DATA] + " " + hour + ":" + minute + ":00"

            MINUTO_ATUAL = int(minute)

        LAST_TIMEDELTA = hour_to_timedelta(data[D_HORA])

    # linha do corpo
    elif altura == 1:

        items = line.strip().split(" ")
        if len(items) == 0 or len(items[0]) == 0: continue

        # testa para ver se eh o sub-header
        if altura == 1 and len(items) > 6:
            c = items[0][0]
            if c >= '0' and c <= '9':
                ip_src_a = items[0].split(".")

                ip_src = ip_src_a[0] + "." + ip_src_a[1] + "." + ip_src_a[2] + "." + ip_src_a[3]
                data[D_SIP] = ip_src
                ip_len = len(ip_src_a)
                if ip_len < 5: continue
                data[D_SPORT] = ip_src_a[4]


                ip_dst_a = items[2].split(".")
                ip_len = len(ip_dst_a)

                # remove o ":" do final dos campos do ip_dst
                ip_dst_a[ip_len-1] = ip_dst_a[ip_len-1] [:-1]

                # reconstitui o ip
                ip_dst = ip_dst_a[0] + "." + ip_dst_a[1] + "." + ip_dst_a[2] + "." + ip_dst_a[3]
                data[D_DIP] = ip_dst

                if ip_len == 4:
                    port_dst = "0"
                else:
                    port_dst = ip_dst_a[4]

                    # concentra portas sem interesse na porta "0"
                    proto_port = data[D_PROTO] + ":" + port_dst

                
                if (data[D_PROTO] + ":" + data[D_SPORT] + ":" + port_dst) == "17:53:53": # conversa entre servidores DNS
                    continue

                elif proto_port == "17:53": # se for dns request
                    if len(items) < 10:# or (items[7] != 'A?' and items[8] != 'A?'):
                        continue

                    if items[6][0] >= '0' and items[6][0] <= '9':
                        query_id = items[6].replace("+", "")
                        query_id = query_id.replace("%", "")

                        query = get_query(items)
                        if query is None: continue


                        src_lat, src_lon, src_client_id = site_from_ip(data[D_SIP])[4:7]
                        target_lat, target_lon, target_client_id = site_from_ip(data[D_DIP])[4:7]


                        if src_client_id == "71" and target_client_id == "71": continue # descarta pacotes cuja origem e o destino sao OTHERS


                        src_key = None
                        if src_client_id != "71":
                            # cadastra chave no tinycubes dict(source)
                            src_key = DATA_HORA_BASE + ";" + src_lat + ";" + src_lon + ";1;" + src_client_id
                            if src_key not in tinycubes_dict: dict_insert(tinycubes_dict, src_key, data[D_SIP], data[D_TTL])
                            elif data[D_SIP]+"-"+data[D_TTL] not in tinycubes_dict[src_key]: dict_insert_ip(tinycubes_dict, src_key, data[D_SIP], data[D_TTL])


                        target_key = None
                        if target_client_id != "71":
                            # cadastra chave no tinycubes dict(target)
                            target_key = DATA_HORA_BASE + ";" + target_lat + ";" + target_lon + ";1;" + target_client_id
                            if target_key not in tinycubes_dict: dict_insert(tinycubes_dict, target_key, data[D_DIP])
                            elif data[D_DIP] not in tinycubes_dict[target_key]: dict_insert_ip(tinycubes_dict, target_key, data[D_DIP])


                        # pareamento dns
                        dns_key = data[D_SIP] + " " + data[D_SPORT] + " " + data[D_DIP] + " " + query_id + " " + query

                        if dns_key not in dns_match:
                            request_time = hour_to_timedelta(data[D_HORA])
                            dns_match_insert(dns_match, dns_key, request_time, data[D_TTL], src_key, target_key, data[D_SIP], data[D_DIP])

                        # query repetida
                        else:
                            '''
                            # contabiliza req repetida origem
                            tinycubes_dict[src_key]["client_as_source"]["total_req_repetida"] += 1
                            tinycubes_dict[src_key][data[D_SIP]+"-"+data[D_TTL]]["total_req_repetida"] += 1

                            # contabiliza req repetida target
                            tinycubes_dict[target_key]["client_as_target"]["total_req_repetida"] += 1
                            tinycubes_dict[target_key][data[D_DIP]]["total_req_repetida"] += 1
                            '''
                            continue


                elif (data[D_PROTO] + ":" + data[D_SPORT]) == "17:53": # dns response

                    if items[6][0] >= '0' and items[6][0] <= '9':
                        query_id = items[6].replace("*", "")
                        query_id = query_id.replace("-", "")
                        query_id = query_id.replace("|", "")
                        query_id = query_id.replace("$", "")

                        query = get_query(items)
                        if query is None: continue
                        
                        dns_key = data[D_DIP] + " " + port_dst + " " + data[D_SIP] + " " + query_id + " " + query

                        if dns_key in dns_match:
                            if dns_match[dns_key][RESPONSE] is None:
                                dns_match[dns_key][RESPONSE] = 1
                        else:
                            #print(f"{data[D_HORA]} {line.strip()}", file=f_resp)
                            pass


dict_flush(tinycubes_dict, dns_match)