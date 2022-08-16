import json
import os
import pickle
import time
from datetime import datetime, timedelta

import pandas as pd
import pygsheets
import requests
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


class API():
    def __init__(self,url=None,API_KEY=None,API_SECRET=None) -> None:
        self.url = url
        self.API_KEY = API_KEY
        self.API_SECRET = API_SECRET
    
    
    def cotacaoDolar(self):
        acesso = requests.get(self.url)
        cotacao = acesso.json()
        return round(float(cotacao['USD']['bid']),2)


    def reportsLifToff(self,url):
        #pra fazer a consulta da API do relatório precisa esperar um pouco pra liftoff processar o relatório. 
        #Então por isso faco um looping em while até o status_code ser igual a 200 
        # e a cada tentativa meu código espera 5seg
        status_code = 0
        while status_code != 200:
            time.sleep(5)
            r = requests.get(url,auth=(self.API_KEY,self.API_SECRET))
            status_code = r.status_code
        return r.text

    #Principal função para consultas a API da lifToff
    def lifToff(self,start_time,end_time):
        #Primeira parte da API
        #  nós precisamos gerar uma solicitação de uma relatório.
        #  Passando algumas informações como o range da data
        #  e nesse caso o relatório vai ter um group by nas colunas expeficicadas abaixo
        r = requests.post(
            self.url,
            json={
                "group_by": ["apps", "campaigns"],
                "start_time": start_time,
                "end_time": end_time,
            },
            auth=(self.API_KEY, self.API_SECRET),
            headers={"Content-Type": "application/json"},
            )
        
        #pegar o id do relatorio solicitado pela API pois ele sera usado no andPoint da proxima API
        id = json.loads(r.content)['id']
        urlReport = f'https://data.liftoff.io/api/v1/reports/{id}/data'
        #consultar a API para pegar o relatório solicitado
        csv = API.reportsLifToff(self,url=urlReport)
        #o retorno da API é um arquivo CSV entao vamos tranformalo em uma lista, separando por ','
        list = csv.split("\n")
        #Transformando o retorno da API em um df o len( 0 ) é o cabeçalho é as próximas linhas da lista seria o corpo
        df = pd.DataFrame([x.split(",") for x in list[1:]],columns=[x.split(",") for x in list[:1]])
        #renomear a coluna  spend por cost para padronizar com a tabela do adjust
        df.rename(columns={'spend':'cost'}, inplace=True)

        return df


class GoogleSheets():
    def __init__(self,clientSecret,credentials,spreadSheetID):
        self.spreadSheetID = spreadSheetID
        spreadSheetID = self.spreadSheetID
        self.credentials = credentials
        self.clientSecret = clientSecret
        clientSecret = clientSecret
        cliente = pygsheets.authorize(service_file = clientSecret)
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']
        self.sht = cliente.open_by_key(spreadSheetID)

    def pull_sheet_data(self,workSheetName):
        creds = GoogleSheets.gsheet_api_check(self)
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=self.spreadSheetID,range=workSheetName).execute()
        values = result.get('values', [])
        
        if not values:
            print(f'No data found : {workSheetName}.')
        else:
            rows = sheet.values().get(spreadsheetId=self.spreadSheetID,range=workSheetName).execute()
            data = rows.get('values')
            return data


    def gsheet_api_check(self):
        creds = None
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials, self.scopes)
                creds = flow.run_local_server(port=0)
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        return creds
    
    
    def insertDataFrameToGsheets(self,workSheetName,dataFrame,clear=False):
        wks = self.sht.worksheet_by_title(workSheetName)
        if clear:
            wks.clear()
        rowCount = len(wks.get_all_records())
        if rowCount == 0:
            wks.set_dataframe(dataFrame,'A1')
        else:
            row_count = len(wks.get_all_records()) + 2
            wks.set_dataframe(dataFrame,(row_count,1),copy_head=False) 


class Data():
    def __init__(self,data=None,dfAdjust=None,dfCustoApp=None):
        self.dfAux=data
        self.adjust = dfAdjust
        self.custoApp = dfCustoApp
    

    def gastosPerformance(self):
        # Criando o data Frame que ira receber os dados que deram retornados.
        dfReturn = pd.DataFrame()
        #Percorrer linha a linha do data Frame que pegamos do google sheets.
        for index,row in self.dfAux.iterrows():
            self.dfAux = pd.DataFrame(row)
            #Crio duas colunas novos no data Frame, os nomes dos app ficam no índex do df
            #  então passo ele para uma coluna e o valor desse passar a ter uma coluna de value.
            self.dfAux['apps']=self.dfAux.index
            self.dfAux['value']=self.dfAux[index]
            
            #Como eu passei o valor do índex para a coluna apps, posso excluir o índex do df.
            self.dfAux.drop(index, inplace=True, axis=1)
            self.dfAux = self.dfAux.reset_index(drop=True)
            
            #Apenas crio as colunas com o mês e ano de referencia 
            dfIndex = self.dfAux[:3]
            self.dfAux['year'] = dfIndex['value'][1]
            self.dfAux['month'] = dfIndex['value'][2]
            self.dfAux = self.dfAux[3:]
            #Faço uma appende de cada linha em um único df pra consolidar tudo
            dfReturn = dfReturn.append(self.dfAux)
        return dfReturn
    
    
    #Calcula para quando o modelo for igual a CPM
    def cpm(self,index):

        #declatando as variaveis para fazer os calculos ficarem mais limpos
        valor =  float(self.custoApp['valor'][index].replace(',','.'))
        valorFixo = float(self.custoApp['valorFixoDia'][index].replace(',','.'))
        cotacaoParceiro = float(self.custoApp['cotacaoDolarParceiro'][index].replace(',','.'))
        # cotacaoDolarAtual = API(url='https://economia.awesomeapi.com.br/all/USD-BRL').cotacaoDolar()
        # impressions = self.adjust['impressions'].fillna(0)
        cost = self.adjust['cost'].fillna(0)

        #calculo do cost2
        self.adjust['cost2'] = (cost*cotacaoParceiro)
        #calculo do cost
        self.adjust['cost'] = valorFixo

        return self.adjust


    def modelos(self,index,modelo):
        if modelo == 'CPM':
            adjust = Data.cpm(self,index=index)  
        elif modelo == 'CPI':
            adjust = Data.cpi(self,index=index)
        elif modelo == 'CPA':
            adjust = Data.cpa(self,index=index)
        return adjust


    def cpa(self,index):

        valor = float(self.custoApp['valor'][index].replace(',','.'))
        first_purchase = self.adjust['first_purchase'].fillna(0)
        
        self.adjust['cost'] = valor*first_purchase
        
        return self.adjust
    

    def cpi(self,index):

        valor = float(self.custoApp['valor'][index].replace(',','.'))
        installs = self.adjust['installs'].fillna(0)

        self.adjust['cost'] = valor*installs

        return self.adjust
    

    def goupBy(df,groupedColumns,sumColumns):
        # #groupBy do df
        # return df.fillna(0).groupby(groupedColumns)[sumColumns].apply(lambda x : x.astype(int).sum())
        # return df.groupby(groupedColumns).agg({'col3':'sum','col4':'sum'})
        columns={}
        for x in sumColumns:
            columns[x]='sum'
        return df.groupby(groupedColumns).agg(columns)


#testando API liftoff
if __name__=='__main__':

    lifToff = API(
        url="https://data.liftoff.io/api/v1/reports",
        API_KEY="c4416825bf",
        API_SECRET="nucqfsPxbHGvdkXtmTmsJg=="
    )    

    df = lifToff.lifToff(start_time="2021-11-01",end_time="2022-11-01")
    print(df)





