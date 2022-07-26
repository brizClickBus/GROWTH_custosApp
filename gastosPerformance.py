import pandas as pd 
from utils import GoogleSheets,Data
import os 

#Estanciando a classe de funções do google sheets.
gastosPerformance = GoogleSheets(spreadSheetID = os.environ.get("gastosPerformanceID"),
                                clientSecret=os.path.join('.','clientID','client_secret.json'))
#Chamando a função para tranformar a planilha :(Gastos Performance 2021/2022) aba :(aux) em uma lista.
aux = gastosPerformance.pull_sheet_data(workSheetName='aux')

#Transformando o retorno da planilha em um dataFrame.
auxDf = pd.DataFrame(aux[1:],columns=aux[0])

#Estanciando a classe de manipulação de dados
dataGastos = Data(data=auxDf)
#Chamando a função que vai ajustar a planilha pra ser inserida no banco
df = dataGastos.gastosPerformance()

#Vamos subir o data Frame na planilha do google sheets
gastosPerformance.insertDataFrameToGsheets(workSheetName='returnAux',dataFrame=df,clear=True)