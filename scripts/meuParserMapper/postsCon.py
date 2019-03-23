import psycopg2

class pgConnection:
    def __init__(self, user="" ,password="", host="127.0.0.1",port="5432", database=""):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.connection = None
        self.cursor = None


    def connect(self):
        if self.user is not '':
            try:
                self.connection = psycopg2.connect(user = "sysadmin",
                                              password = "pynative@#29",
                                              host = "127.0.0.1",
                                              port = "5432",
                                              database = "postgres_db")
                self.cursor = self.connection.cursor()
                # Print PostgreSQL Connection properties
                print ( self.connection.get_dsn_parameters(),"\n")
                # Print PostgreSQL version
                self.cursor.execute("SELECT version();")
                record = self.cursor.fetchone()
                print("Você está conectado a - ", record,"\n")
            except (Exception, psycopg2.Error) as error :
                print ("Erro ao conectar com o PostgreSQL", error)
            finally:
                #closing database connection.
                    if(self.connection is not None):
                        self.cursor.close()
                        self.connection.close()
                        print("Conexão com PostgreSQL fechada!")
        else:
            print("Usuário não definido!")






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