import postsCon

class myColumnFormat:
    def __init__(self, format:str):
        self.createFormat = format.strip(' ')
        self._tupleFormat = None

    @property
    def tupleFormat(self):
        lstAux = self.createFormat.split(',')
        stringReturn = ''
        for elem in lstAux:
            elem = elem.strip(' ')
            #Elem return recebe o resultado do split
            #Que é somente o nome do atributo, ignorando o tipo (ex INTEGER)
            lstSplit = elem.split(' ')
            stringAux = lstSplit[0]
            stringReturn = stringReturn+stringAux+', '
        self._tupleFormat = stringReturn.strip(', ')
        return self._tupleFormat


class myParser:
    def __init__(self):
        self.lstValues = []
        self.conn = postsCon.pgConnection()
        self.header = ''

    def getHeader(self, csvPath):
        with open(csvPath, 'r') as csvFile:
            headerStr = csvFile.readline()
            with open('header.txt', 'w') as file:
                file.write(headerStr)

    def csvCreationProtocol(self, csvPath, columnFormat:myColumnFormat, tableName):
        sql = '''CREATE TABLE '''+tableName+'''('''+columnFormat.createFormat+''');'''
        print(sql)
        self.conn.sqlExec(sql)

        with open(csvPath, 'r') as csvFile:
            headerStr = csvFile.readline()
            print('header',headerStr)

            rowAux = csvFile.readline()
            print('Inserindo dados. . .')
            while rowAux is not "":
                rowAux = rowAux.split(';')
                lstAux = []
                for value in rowAux:
                    value = value.replace("'", '')
                    value = value.replace('"', '')
                    value = value.replace('""', '')
                    lstAux.append(value)

                if len(lstAux)>53:
                    self.concatValues(lstAux)

                # lstAux[19] = lstAux[19].replace(',', '.')
                lstAux[52] = lstAux[52].replace(',', '.').replace('\n', '')
                lstAux = tuple(lstAux)
                sql = '''INSERT INTO '''+tableName+''' ('''+columnFormat.tupleFormat+''') VALUES '''+str(tuple(lstAux))+''';'''

                if self.conn.sqlExec(sql) is False:
                    print(sql)
                    break
                del (lstAux)
                rowAux = csvFile.readline()

    def concatValues(self,lst):
        if len(lst) == 54:
            print('concatValues - len(lst):', len(lst))
            value = str(lst[29]) + str(lst[30])
            lst[29] = value
            del (lst[30])
            print('String corrigida: ', lst[29])
        elif len(lst) == 55:
            print(lst)
            print('concatValues - len(lst):', len(lst))
            value = str(lst[29]) + str(lst[30]) + str(lst[31])
            lst[29] = value
            del (lst[29:31])
            print('String corrigida: ', lst[29])
        # print('concatValues - len(lst):', len(lst))
        # if len(lst) == 23:
        #     value = str(lst[17]) + str(lst[18])
        #     lst[17] = value
        #     del(lst[18])
        #     print('String corrigida: ', lst[17])
        # elif len(lst) == 24:
        #     print('Lista a ser corrigida: ',lst)
        #     value = str(lst[16]) + str(lst[17])+str(lst[18])
        #     lst[16] = value
        #     del(lst[16:18])
        #     print('Lista corrigida: ',lst)
        #     print('String corrigida: ', lst[15])


def main():
    header2014 = 'cod_eleicao VARCHAR(20), ' \
             'descricao_eleicao VARCHAR(30), ' \
             'dt_hr DATE, ' \
             'cnpj_prestador VARCHAR(16), ' \
             'sequencial_candidato VARCHAR(30), ' \
             'UF VARCHAR(3), ' \
             'sigla_partido VARCHAR(20), ' \
             'num_candidato INTEGER, ' \
             'cargo VARCHAR(40), ' \
             'nome_candidato VARCHAR(100), ' \
             'cpf_candidato VARCHAR(16), ' \
             'tipo_documento VARCHAR(120), ' \
             'num_documento VARCHAR(60), ' \
             'cpf_cnpj_fornecedor VARCHAR(16), ' \
             'nm_fornecedor VARCHAR(200), ' \
             'nm_fornecedorRF VARCHAR(200), ' \
             'cod_setor_fornecedor VARCHAR(12), ' \
             'setor_economico_fornecedor VARCHAR(400), ' \
             'dt_despesa DATE,  ' \
             'vr_despesa FLOAT, ' \
             'tipo_despesa VARCHAR(400), ' \
             'descricao_despesa VARCHAR(400)'

    header2018 = 'DT_GERACAO DATE,' \
                 'HH_GERACAO TIME,' \
                 'ANO_ELEICAO INTEGER,' \
                 'CD_TIPO_ELEICAO INTEGER,' \
                 'NM_TIPO_ELEICAO VARCHAR(100),' \
                 'CD_ELEICAO INTEGER,' \
                 'DS_ELEICAO VARCHAR(35),' \
                 'DT_ELEICAO TIMESTAMP,' \
                 'ST_TURNO INTEGER,' \
                 'TP_PRESTACAO_CONTAS VARCHAR(30),' \
                 'DT_PRESTACAO_CONTAS DATE,' \
                 'SQ_PRESTADOR_CONTAS VARCHAR(100),' \
                 'SG_F VARCHAR(2),' \
                 'SG_UE VARCHAR(2),' \
                 'NM_UE VARCHAR(30),' \
                 'NR_CNPJ_PRESTADOR_CONTA VARCHAR(34),' \
                 'CD_CARGO INTEGER,' \
                 'DS_CARGO VARCHAR(30),' \
                 'SQ_CANDIDATO VARCHAR(100),' \
                 'NR_CANDIDATO INTEGER,' \
                 'NM_CANDIDATO VARCHAR(100),' \
                 'NR_CPF_CANDIDATO VARCHAR(30),' \
                 'NR_CPF_VICE_CANDIDATO VARCHAR(30),' \
                 'NR_PARTIDO INTEGER,' \
                 'SG_PARTIDO VARCHAR(100),' \
                 'NM_PARTIDO VARCHAR(100),' \
                 'CD_TIPO_FORNECEDOR INTEGER,' \
                 'DS_TIPO_FORNECEDOR VARCHAR(30),' \
                 'CD_CNAE_FORNECEDOR INTEGER,' \
                 'DS_CNAE_FORNECEDOR VARCHAR(200),' \
                 'NR_CPF_CNPJ_FORNECEDOR VARCHAR(30),' \
                 'NM_FORNECEDOR VARCHAR(200),' \
                 'NM_FORNECEDOR_RFB VARCHAR(200),' \
                 'CD_ESFERA_PART_FORNECEDOR VARCHAR(30),' \
                 'DS_ESFERA_PART_FORNECEDOR VARCHAR(200),' \
                 'SG_UF_FORNECEDOR VARCHAR(100),' \
                 'CD_MUNICIPIO_FORNECEDOR VARCHAR(100),' \
                 'NM_MUNICIPIO_FORNECEDOR VARCHAR(100),' \
                 'SQ_CANDIDATO_FORNECEDOR VARCHAR(100),' \
                 'NR_CANDIDATO_FORNECEDOR VARCHAR(100),' \
                 'CD_CARGO_FORNECEDOR INTEGER,' \
                 'DS_CARGO_FORNECEDOR VARCHAR(200),' \
                 'NR_PARTIDO_FORNECEDOR INTEGER,' \
                 'SG_PARTIDO_FORNECEDOR VARCHAR(100),' \
                 'NM_PARTIDO_FORNECEDOR VARCHAR(200),' \
                 'DS_TIPO_DOCUMENTO VARCHAR(200),' \
                 'NR_DOCUMENTO VARCHAR(200),' \
                 'CD_ORIGEM_DESPESA VARCHAR(100),' \
                 'DS_ORIGEM_DESPESA VARCHAR(300),' \
                 'SQ_DESPESA INTEGER,' \
                 'DT_DESPESA DATE,' \
                 'DS_DESPESA VARCHAR(300),' \
                 'VR_DESUPESA_CONTRATADA FLOAT'

    # columnFormat = myColumnFormat(header2014)
    # tableName = 'despesas2014'
    # table = myParser()
    # table.csvCreationProtocol("C:/Users/Hugo/Desktop/BD2/SB/despesas_candidatos_2014_brasil.txt", columnFormat, tableName)

    columnFormat = myColumnFormat(header2018)
    tableName = 'despesas2018'
    table = myParser()
    # table.getHeader("C:/Users/Hugo/Desktop/BD2/despesas_contratadas_candidatos_2018_BRASIL.csv")
    table.csvCreationProtocol("C:/Users/Hugo/Desktop/BD2/despesas_contratadas_candidatos_2018_BRASIL.csv", columnFormat, tableName)

main()
