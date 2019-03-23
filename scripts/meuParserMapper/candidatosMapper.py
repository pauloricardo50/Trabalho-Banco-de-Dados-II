import psycopg2
con = psycopg2.connect(host='localhost', database='regiao',
user='postgres', password='postgres123')
cur = con.cursor()
sql = 'create table cidade (id serial primary key, nome varchar(100), uf varchar(2))'
cur.execute(sql)
sql = "insert into cidade values (default,'São Paulo,'SP')"
cur.execute(sql)
con.commit()
cur.execute('select * from cidade')
recset = cur.fetchall()
for rec in recset:
    print (rec)
con.close()





class Conexao(object):
    _db=None
    def __init__(self, mhost, db, usr, pwd):
        self._db = psycopg2.connect(host=mhost, database=db, user=usr,  password=pwd)
    def manipular(self, sql):
        try:
            cur=self._db.cursor()
            cur.execute(sql)
            cur.close()
            self._db.commit()
        except:
            return False
        return True

    def consultar(self, sql):
        rs=None
        try:
            cur=self._db.cursor()
            cur.execute(sql)
            rs=cur.fetchall()
        except:
            return None
        return rs

    def proximaPK(self, tabela, chave):
        sql='select max('+chave+') from '+tabela
        rs = self.consultar(sql)
        pk = rs[0][0]
        return pk+1

    def fechar(self):
        self._db.close()