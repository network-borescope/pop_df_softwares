"""

Programa que gera arquivos .csv no padrão tinycubes a partir de dados coletados no pop df

Le os pacotes IP com protocolo TCP, flag SYN e qq porta e salva uma linha com:

data-hora (resolução de minuto); lat; lon; id_pop (sempre 1); id_cliente; ttl; distancia; tp_servico; id_destino; quantidade_de_pacotes

2021-08-24 18:58:00;-15.795423;-47.873152;1;58;59;5;1;18;2

As linhas são grupadas em arquivos de cada minuto.
 
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

def build_data_hora(d_data, d_hora, d_min):
    d = d_data+" "+d_hora+":"+d_min+":00"
    return d

def date_to_day(d_data):
    year, month, day = d_data.split("-")
    day = datetime.datetime(year=int(year), month=int(month), day=int(day)).weekday()

    day_of_week = {0: "Segunda", 1: "Terca", 2: "Quarta", 3: "Quinta", 4: "Sexta", 5: "Sabado", 6: "Domingo"}
    #return day_of_week[day]
    return str((day+1 % 7)+1)


def hour_to_timedelta(d_hora):
    hour, min, sec = d_hora.split(":")

    return datetime.timedelta(hours=int(hour), minutes=int(min), seconds=float(sec))

# pega query e query id do request
def request_parser(items, data):
    if len(items) < 10 or (items[7] != 'A?' and items[8] != 'A?'):
        data.clear()
        return False

    # verifica se tem flags
    pos = 7
    flags = items[pos]
    if flags[0] != '[':
        flags = ""
        pos -= 1
    query = items[pos+2][:-1] # remove o ponto do final da query
    data[D_QUERY] = query

    if items[6][0] >= '0' and items[6][0] <= '9': # assume que items[6] eh o query id
        query_id = items[6].replace("+", "")
        query_id = query_id.replace("%", "")
        data[D_QUERY_ID] = query_id
    else:
        print("#######################################", items[6])
        data.clear()
        return False

    return True

# pega query e query id do response
def response_parser(items, data, cnames):
    if items[6][0] >= '0' and items[6][0] <= '9':
        query_id = items[6].replace("*", "")
        query_id = query_id.replace("-", "")
        query_id = query_id.replace("|", "")
        query_id = query_id.replace("$", "")
        data[D_QUERY_ID] = query_id
    else:
        data.clear()
        return False

    n = len(items)
    i = 0
    while i < n:
        if items[i] == 'q:':
            i += 1
            if not (i < n and items[i] == "A?"):
                data.clear()
                return False

            i += 1
            data[D_QUERY] = items[i][:-1] # remove o ponto


            i += 1
            if not (i < n and items[i][0] != "0"):
                data.clear()
                return False

            i += 1
            if not (i < n): data.clear(); return False
            s = items[i][:-1] # remove o ponto
            if s != data[D_QUERY]: data.clear(); return False
            data[D_QUERY_POS] = i

            i += 1
            if not (i < n and items[i][0] == '['): data.clear(); return False

            i += 1
            if not (i < n): data.clear(); return False

            if items[i] == "A":
                return True

            data.clear()
            #if items[i] == "CNAME": return "CNAME"
            if items[i] == "CNAME":
                while i < n:
                    # name validade CNAME name
                    i += 1
                    if not i < n: return "CNAME"

                    cname = items[i]
                    if cname[len(cname)-1] == ",":
                        cname = cname[:-2] # remove a virgula e o ponto
                        cnames.add(cname)
                    else:
                        cname = cname[:-1] # remove o ponto
                        cnames.add(cname)
                        return "CNAME"

                    i += 1 # name
                    if not i < n: return "CNAME"

                    i += 1 # validade
                    if not i < n: return "CNAME"

                    i += 1 # CNAME
                    if not (i < n and items[i] == "CNAME"): return "CNAME"

                return "CNAME"

            return False

        i += 1

    data.clear()
    return False

def get_response_ips(items, know_ips, data):
    # query validade A ip
    n = len(items)
    i = data[D_QUERY_POS]


    continua = True
    while i < n and continua:
        ignora = False
        # pega a query
        #s = items[i][:-1] # remove o ponto da query
        if i >= n: break
        if items[i][:-1] != data[D_QUERY]: ignora = True

        i += 1
        # ignora validade da resposta

        i += 1
        if i >= n: break
        if not (items[i] == 'A'): ignora = True

        i += 1 # posicao do ip
        if i >= n: break
        if not (items[i] >= '0' and items[i] <= '9'): ignora = True

        ip = items[i]
        continua = False
        if ip[-1] == ',':
            ip = ip[:-1]
            continua = True

        if not ignora: know_ips[ip] = data[D_QUERY]

        i += 1

def main():

    minuto_base = -1 # armazena o minuto de inicio da captura

    count = 0
    #fin = open(filename, "r")
    fin = stdin

    services = {
        "53": "7",
        "80": "1", "443": "1",
        "25": "2", "587": "2", "465": "2",
        "110": "3", "995": "3",
        "143": "4",
        "20": "5", "21": "5",
        "22": "6"
    }

    key_count = {} # tuplas/padroes: dia-da-semana, hora, id_cliente, ip_origem, distancia, ttl, porta_destino (servico), id_destino (0 = qualquer) : count
    #know_ips = {} # { ip: hostname }
    #cnames = set() # cname list
    #dns_a_count = 0
    #dns_a_cname_count = 0

    dict_dst = {}


    data = []

    prev_fout_name = None
    fout = None

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
        #print (altura)

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
            data = [ items[0], items[1], items[6].strip(","), val_proto, items[8].strip(","), "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0" ]
            #print("Data> ", data)
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

                        # concentra portas sem interesse na porta "0"
                        proto_port = data[D_PROTO] + ":" + port_dst

                    rede = site_from_ip(data[D_SIP])
                    client_id = rede[6]
                    
                    # Nilson - ignore client==71 
                    if int(client_id) == 71: continue
                    
                    lat = rede[4]
                    lon = rede[5]

                    destination_id = db_manager_v2.get_or_insert_ip(conn, data[D_DIP])

                    #service = "DESCONHECIDO"
                    service = data[D_DPORT]
                    if data[D_DPORT] in services: service = services[data[D_DPORT]]
                    else: service = "9"

                    hour, minute, sec = data[D_HORA].split(":")

                    if minuto_base == -1: minuto_base = minute
                    # dia-da-semana, hora, id_cliente, ip_origem, distancia, ttl, porta_destino (servico), id_destino (0 = qualquer)

                    day = date_to_day(data[D_DATA])
                    prefix = build_data_hora(data[D_DATA], hour, minute)+";"


                    #key = ""
                    #key += day + ","
                    #key += hour + ","
                    key = prefix
                    key += lat + ";"
                    key += lon + ";"
                    key += "1" + ";"
                    key += client_id + ";"
                    key += data[D_TTL] + ";"
                    key += data[D_DIST] + ";"
                    key += service + ";"
                    key += str(destination_id)
                    #key += data[D_SIP] + ","
                    #key += data[D_DIST] + ","
                    #key += data[D_TTL] + ","
                    #key += data[D_DPORT] + ","

                    #key += data[D_DIP] + ","





                    nn5_int = (int(minute) // 5) * 5
                    nn5 = "%02d"%(nn5_int)

                    yy, mm, dd = data[D_DATA].split("-")
                    fout_name = "./csv_serv/%s%s%s_%s_%s.csv"%(yy,mm,dd,hour,minute)
                    if fout_name != prev_fout_name:
                        for key in key_count:
                            print(key + ";" + str(key_count[key]), file=fout)
                        key_count = {}
                        if fout is not None: fout.close()
                        prev_fout_name = fout_name
                        fout = open(fout_name,"wt")

                    if key not in key_count:
                        key_count[key] = 1
                    else:
                        key_count[key] += 1

                    #print(key + ";" + str(key_count[key]), file=fout)

    conn.close()

    with open(fout_name, "wt") as fout:
        #
        # ajustado por nilson: garante ordem crescente de datas
        #
        #items = []
        for key in key_count:
            print(key + ";" + str(key_count[key]), file=fout)

            # items.append(key + ";" + str(key_count[key]))
        #sorted_items = sorted(items)
        #for line in sorted_items:
        #    print(line, file=fout)
    fout.close()
if __name__ == '__main__':
    main()
