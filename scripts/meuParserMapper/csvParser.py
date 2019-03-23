
class ValueType:
    def __init__(self):
        pass

class myColumn:
    def __init__(self):
        self.valueName = ''
        self.valueType = ''



class myValue:
    def __init__(self):
        self.column = myColumn()
        self.value = ''

    def __str__(self):
        return self.value



class myTable:
    def __init__(self):
        self.lstValues = []
        self.Cols = []

        self.header = ''
        self.Cols = []

    @property
    def numCols(self):
        return len(self.Cols)

    def csvCreationProtocol(self, csvPath):
        pass

    def loadCsv(self, csvPath):
        with open(csvPath, 'r') as csvFile:

            headerStr = csvFile.readline()
            self.header = headerStr.split(";")
            print(self.header)

            for elem in self.header:

                valueAux = myColumn()
                valueAux.valueName = elem
                self.Cols.append(valueAux)

            rowAux = csvFile.readline().split(';')
            while rowAux is not "":
                print(rowAux)
                for value in rowAux:
                    for column in self.Cols:
                        valueAux = myValue()
                        valueAux.column = column
                        valueAux.value = value
                        self.lstValues.append(valueAux)
                rowAux = csvFile.readline().split(';')



def main():
    table = myTable()
    table.loadCsv("C:/Users/Hugo/Desktop/BD2/despesas_candidatos_2014_brasil.txt")
    print(table.lstValues)

main()






