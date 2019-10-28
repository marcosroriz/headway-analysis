import csv
from geopy.distance import vincenty

estados = {}
cidades = {}

nomearqestado = open("C:/Users/marco/Desktop/estados.csv", encoding='utf-8')
leitorarqestado = csv.DictReader(nomearqestado)

for linha in leitorarqestado:
    codigo_uf = int(linha["codigo_uf"])
    nome = linha["nome"]

    estados[codigo_uf] = nome

nomearqmunicipios = open("C:/Users/marco/Desktop/municipios.csv", encoding='utf-8')
leitorarqmunicipios = csv.DictReader(nomearqmunicipios)

for linha in leitorarqmunicipios:
    codigo_ibge = int(linha["codigo_ibge"])
    codigo_uf = int(linha["codigo_uf"])
    nome = linha["nome"]
    capital = int(linha["capital"])
    latitude = float(linha["latitude"])
    longitude = float(linha["longitude"])

    cidades[codigo_ibge] = {
        "codigo_ibge": codigo_ibge,
        "codigo_uf": codigo_uf,
        "estado": estados[codigo_uf],
        "nome": nome,
        "capital": capital,
        "latitude": latitude,
        "longitude": longitude
    }

cod_mun_base = 1721000
lat_base = cidades[cod_mun_base]["latitude"]
lng_base = cidades[cod_mun_base]["longitude"]
loc_base = (lat_base, lng_base)

for cid_cod_ibge, cidade_detalhe in cidades.items():
    loc_cidade = (cidade_detalhe["latitude"], cidade_detalhe["longitude"])
    dist_base = vincenty(loc_base, loc_cidade).meters

    cidade_detalhe["distancia"] = dist_base

cidades_ordenada = sorted(cidades.items(), key=lambda k: k[1]["distancia"])

nomearqsaida = "C:/Users/marco/Desktop/distancias.csv"
with open(nomearqsaida, 'w', newline='') as arqsaida:
    header = ["codigo_uf", "estado", "codigo_ibge", "nome", "capital", "latitude", "longitude", "distancia"]
    writer = csv.DictWriter(arqsaida, delimiter=',', fieldnames=header)
    writer.writeheader()

    for cidade in cidades.values():
        writer.writerow(cidade)


print("TERMINEI")