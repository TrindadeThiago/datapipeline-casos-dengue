import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [
    "id", "data_iniSE", "casos", "ibge_code", "cidade", "uf", "cep", "latitude", "longitude"
]

def lista_para_dicionario(elemento, colunas):
    """
        Recebe 2 listas
        Retorna 1 dicionario
    """
    return dict(zip(colunas, elemento))

def texto_para_lista(elemento, delimitador="|"):
    """
        Recebe um texto e um delimitador
        Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)

def trata_datas(elemento):
    """
        Recebe dicionario e cria um novo campo ANO_MES
        Retorna o mesmo dicionario com o novo campo
    """
    elemento["ano_mes"] = "-".join(elemento["data_iniSE"].split("-")[:2])
    return elemento

def chave_uf(elemento):
    """
        Recebe dicionario 
        Retorna uma tupla com estado(UF) e o elemento(UF, dicionario)
    """
    chave = elemento["uf"]
    return (chave, elemento)

def casos_dengue(elemento):
    """
        Recebe um tupla ('UF', [{}, {}] )
        Retorna uma tupla ('UF-YYYY-MM, 0.0)
    """
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)

def chave_uf_ano_mes_de_lista(elemento):
    """
        Recebe uma lista de elementos
        Retorna uma tupla contendo chave e valor de chuva em mm
        ('UF-ANO-MES', 1.3)
    """
    data, mm, uf = elemento
    ano_mes = "-".join(data.split("-")[:2])
    chave = f"{uf}-{ano_mes}"
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm

def arredonda(elemento):
    """
        Recebe uma tupla
        Retorna uma tupla com valor arredondado
    """
    chave, mm = elemento
    return (chave, round(mm, 1))

def filtra_campos_vazios(elemento):
    """
        Remove elementos que tenham chaves vazias
        Recebe uma tupla
        Retorna a mesma tupla, com o filtro aplicado
    """
    chave, dados = elemento
    if all([
        dados['chuvas'],
        dados['dengue']
    ]):
        return True
    return False

def descompactar_elementos(elemento):
    """
        Recebe uma tupla ('CE-2015-01', {'chuvas: [85.8], 'dengue': [175]})
        Retorna uma tupla ('CE', '2015', '11', '0.4', '21.0')
    """
    chave, dados = elemento
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, ano, mes, str(chuva), str(dengue)

def preparar_csv(elemento, delimitador=';'):
    """
        Recebe uma tupla ('CE', '2015', '11', '0.4', '21.0')
        Retorna uma string delimitada "CE;2015;11;0.4;21.0"
    """
    return f"{delimitador}".join(elemento)

dengue = (
    pipeline
    | "Leitura do dataset de Dengue" >> ReadFromText("sample_casos_dengue.txt", skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionario" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Cria campo ano_mes" >> beam.Map(trata_datas)
    | "Cria cria chave pelo estado(UF)" >> beam.Map(chave_uf)
    | "Agrupa por estado(UF)" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Soma os casos pela chave" >> beam.CombinePerKey(sum)
    # | "Mostrar resultados dos casos de dengue" >> beam.Map(print)
)

chuvas = (
    pipeline
    | "Leitura do dataset de Chuvas" >> ReadFromText("sample_chuvas.csv", skip_header_lines=1)
    | "De texto para lista (chuvas)" >> beam.Map(texto_para_lista, delimitador=",")
    | "Criando chave UF-ANO-MES " >> beam.Map(chave_uf_ano_mes_de_lista)
    | "Soma o total de chuva pela chave" >> beam.CombinePerKey(sum)
    | "Arredondar resultados de chuvas" >> beam.Map(arredonda)
    # | "Mostrar resultados de chuvas" >> beam.Map(print)
)

resultado = (
    ({ "chuvas": chuvas, "dengue": dengue })
    | "Mesclar pcollection" >> beam.CoGroupByKey()
    | "Filtrar dados vazio" >> beam.Filter(filtra_campos_vazios)
    | "Descompactar elementos" >> beam.Map(descompactar_elementos)
    | "Preparar csv" >> beam.Map(preparar_csv)
    # | "Mostrar resultados" >> beam.Map(print)
)

header = 'UF;ANO;MES;CHUVA;DENGUE'

resultado | "Criar arquivo csv" >> WriteToText('resultado', file_name_suffix='.csv', header=header)

pipeline.run()