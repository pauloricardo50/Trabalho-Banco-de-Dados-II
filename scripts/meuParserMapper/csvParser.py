import datetime
import postsCon


class myParser:
    def __init__(self):
        self.lstValues = []
        self.conn = postsCon.pgConnection()
        self.header = ''


    def csvCreationProtocol(self, csvPath, header = None, header2 =None, tableName = 'Despesas2014'):
        sql = '''CREATE TABLE despesas2014 ('''+header+''');'''
        print(sql)
        self.conn.sqlExec(sql)

        with open(csvPath, 'r') as csvFile:
            headerStr = csvFile.readline()
            print('header',headerStr)


            rowAux = csvFile.readline().split(';')
            print('Inserindo dados. . .')
            while rowAux is not "":
                # print(rowAux)
                lstAux = []
                for value in rowAux:
                    value = value.replace("'", '')
                    value = value.replace('"', '')
                    value = value.replace('""', '')
                    lstAux.append(value)
                if len(lstAux)>22:
                    self.concatValues(lstAux)

                lstAux[19] = lstAux[19].replace(',', '.')
                lstAux = tuple(lstAux)

                sql = '''INSERT INTO despesas2014 '''+header2+''' VALUES '''+str(tuple(lstAux))+''';'''

                if self.conn.sqlExec(sql) is False:
                    print(sql)
                    break
                del (lstAux)
                rowAux = csvFile.readline().split(';')

    def concatValues(self,lst):
        print('concatValues - len(lst):', len(lst))
        if len(lst) == 23:
            value = str(lst[17]) + str(lst[18])
            lst[17] = value
            del(lst[18])
            print('String corrigida: ', lst[17])
        elif len(lst) == 24:
            print('Lista a ser corrigida: ',lst)
            value = str(lst[16]) + str(lst[17])+str(lst[18])
            lst[16] = value
            del(lst[16:18])
            print('Lista corrigida: ',lst)
            print('String corrigida: ', lst[15])


def main():
    header = 'cod_eleicao VARCHAR(20), ' \
             'descricao_eleicao VARCHAR(30), ' \
             'dt_hr DATE, ' \
             'cnpj_prestador VARCHAR(16), ' \
             'sequencial_candidato VARCHAR(30), ' \
             'UF VARCHAR(3), ' \
             'sigla_partido VARCHAR(8), ' \
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

    header2 = '(cod_eleicao, descricao_eleicao, dt_hr, cnpj_prestador, sequencial_candidato, UF, sigla_partido, num_candidato, cargo, nome_candidato, cpf_candidato, tipo_documento, num_documento, cpf_cnpj_fornecedor, nm_fornecedor, nm_fornecedorRF, cod_setor_fornecedor, setor_economico_fornecedor, dt_despesa, vr_despesa, tipo_despesa, descricao_despesa)'

    table = myParser()
    table.csvCreationProtocol("C:/Users/Hugo/Desktop/BD2/SB/despesas_candidatos_2014_brasil.txt", header, header2)


main()






