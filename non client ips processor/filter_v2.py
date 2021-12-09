"""

Programa que gera os arquivos .csv, out e hist, a partir de dados coletados no pop df.

Arquivo out:
Le os pacotes IP com protocolo TCP, flag SYN e qq porta e salva uma linha com:

data, dia_da_semana, hora, minuto_base, id_cliente, ip_origem, distancia, ttl, porta_destino, servico, id_destino, quantidade_de_pacotes
2021-08-24,3,19,17,40,200.130.147.253,6,122,443,HTTP,695,2

Eh gerado um unico arquivo para aquela coleta.

Arquivo ips:
Le os pacotes IP com protocolo TCP, flag SYN e qq porta e salva uma linha com:

data, hora, minuto_base, id_cliente, ip_origem, qtd_de_pacotes

Eh gerado um unico arquivo para aquela coleta.

"""
import datetime
from ip_to_nome_lat_lon import site_from_ip
from sys import argv, exit, stdin
import db_manager_v2

# GLOBALS

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
D_DPORT = 14
D_QUERY_ID = 15
D_QUERY_POS = 16

def date_to_day(d_data):
    year, month, day = d_data.split("-")
    day = datetime.datetime(year=int(year), month=int(month), day=int(day)).weekday()

    #day_of_week = {0: "Segunda", 1: "Terca", 2: "Quarta", 3: "Quinta", 4: "Sexta", 5: "Sabado", 6: "Domingo"}
    #return day_of_week[day]
    return str((day+1 % 7)+1)


def hour_to_timedelta(d_hora):
    hour, min, sec = d_hora.split(":")
    
    return datetime.timedelta(hours=int(hour), minutes=int(min), seconds=float(sec))

def main():
    arguments = argv[1:]

    if len(arguments) == 1:
        filename = arguments[0]
    else: exit(1)

    filename = filename.split(".")[0]
    minuto_base = -1 # armazena o minuto de inicio da captura

    count = 0

    fin = stdin

    services = {"53": "DNS", "80": "HTTP", "443": "HTTP", "25": "SMTP", "587": "SMTP", "465": "SMTP", "110": "POP3", "995": "POP3", "143": "IMAP", "20": "FTP", "21": "FTP", "22": "SSH"}

    key_count = {} # tuplas/padroes: dia-da-semana, hora, id_cliente, ip_origem, distancia, ttl, porta_destino (servico), id_destino (0 = qualquer) : count

    dict_ips = {} 


    data = []

    conn = db_manager_v2.create_connection("syn_dns_ether.db")
    for line in fin:
        count+=1
        if count % 1000000 == 0: print(count)

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

            clean_line = line.strip()
            items = clean_line.split(" ")

            if len(items) < 6: continue
            if items[2] != "IP": continue

            n = len(items)

            # [data, hora, val_ttl, val_proto, val_ip_id ]
            val_proto = items[15][1:-2]
            data = [ items[0], items[1], items[6].strip(","), val_proto, items[8].strip(","), "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0" ]

            if data[D_TTL] == "oui":
                data = []
                continue

            dist = int(data[D_TTL])
            if dist < 64: dist = 64 - dist
            elif dist < 128: dist = 128 - dist
            else: dist = 255 - dist
            data[D_DIST] = str(dist)

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
                        data[D_DPORT] = port_dst
                    
                    client_id = site_from_ip(data[D_SIP])[6]
                    
                    if client_id != 71: # descarta do arquivo "out" pacotes cuja origem eh OTHERS
                        destination_id = db_manager_v2.get_or_insert_ip(conn, data[D_DIP])

                        service = data[D_DPORT]
                        if data[D_DPORT] in services: service = services[data[D_DPORT]]

                        hour, minute, sec = data[D_HORA].split(":")

                        if minuto_base == -1: minuto_base = minute
                        # dia-da-semana, hora, id_cliente, ip_origem, distancia, ttl, porta_destino (servico), id_destino (0 = qualquer)

                        day = date_to_day(data[D_DATA])
                        prefix = data[D_DATA] + "," + day + "," + hour + "," + minuto_base + ","

                        key = prefix
                        key += client_id + ","
                        key += data[D_SIP] + ","
                        key += data[D_DIST] + ","
                        key += data[D_TTL] + ","
                        key += data[D_DPORT] + ","
                        key += service + ","
                        #key += data[D_DIP] + ","
                        key += str(destination_id)
                        
                        if key not in key_count:
                            key_count[key] = 1
                        else:
                            key_count[key] += 1
                    
                    key1 = data[D_DATA] + "," + hour + "," + minuto_base + "," + client_id + "," + data[D_SIP]

                    if key1 not in dict_ips:
                        dict_ips[key1] = 1
                    else:
                        dict_ips[key1] += 1

    conn.close()
    with open("filter_v2/out_" + filename + ".txt", "w") as fout:
        for key in key_count:
            print(key + "," + str(key_count[key]), file=fout)

    with open("filter_v2/ips_" + filename + ".txt", "w") as fout:
        for key in dict_ips:
            print(key + "," + str(dict_ips[key]), file=fout)


if __name__ == '__main__':
    main()
