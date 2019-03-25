import psycopg2


class pgConnection:
    def __init__(self, user="postgres" ,password="123456", host="127.0.0.1",port="5432", database="Trabalho1"):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.connection = None
        self.cursor = None
        self.__connect__()

    def __connect__(self):
        if self.user is not '':
            try:
                self.connection = psycopg2.connect(user = self.user,
                                              password = self.password,
                                              host = self.host,
                                              port = self.port,
                                              database = self.database)
                self.cursor = self.connection.cursor()

                # print dos parâmetros de conexão e versão do postgres:
                print ( self.connection.get_dsn_parameters(),"\n")
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

    def sqlExec(self, sql:str):
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            return True

        except Exception as Exp:
            print("Ocorreu um erro ao executar a transação!", Exp)
            self.cursor.close()
            self.connection.close()
            return False

