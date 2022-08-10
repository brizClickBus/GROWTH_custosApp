
import os
import pandas as pd
from utils import Data,GoogleSheets


#Estanciando a classe de funções do google sheets.
matrizParceiros = GoogleSheets(spreadSheetID = os.environ.get("matrizParceirosID"),
                                credentials = os.path.join('.','clientID','credentials.json'),
                                clientSecret=os.path.join('.','clientID','clientSecret.json'))
#Chamando a função para tranformar a planilha : (Matriz de Parceiros - Mídia ) aba : (CustosApp_atual) em uma lista.
custosAppAtual:list = matrizParceiros.pull_sheet_data(workSheetName='CustosApp_atual')
#Transformando o retorno da planilha em um dataFrame.
custosAppAtual:pd.DataFrame = pd.DataFrame(custosAppAtual[1:],columns=custosAppAtual[0])

#Lendo o arquivo CSV extraido do adjust.
consolidatedAdjust = pd.read_csv(os.path.join('.','files',os.listdir(os.path.join('.','files'))[0]))

#groupBy do df
consolidatedAdjust = Data.goupBy(
    df=consolidatedAdjust,
    groupedColumns=["source","os_name","campaign"],
    sumColumns=["impressions","clicks","cost",
        "installs","sessions","daus",
        "maus","splash_app_open","search",
        "seat","passenger","checkout",
        "success","first_purchase","revenue"])

#Transformar a coluna de data do arquivo do adjunst tambem em dateTime 
# (Dessa maneira vai ficar mais facil manipular os dois arquivos utilizando filtros das colunas de datas)
consolidatedAdjust['created_at'] = pd.to_datetime(consolidatedAdjust['created_at'])

#Percorrendo por cada linha da planilha de custosApp_Atual.
results = pd.DataFrame()
for index,row in custosAppAtual.iterrows():
    #Transpondo a linha do dataFrame.
    custoApp = pd.DataFrame(row).T
    
    #Transformando a coluna de data que estava em string para DateTime.
    custoApp['dataInicio'] = pd.to_datetime(custoApp['dataInicio'],format='%d/%m/%Y')
    
    #Filtrar o data Frame do arquivo consolidatedAdjust para que conseguimos manipular o mesmo de uma maneira mais rapida.
    if custoApp['origem'][index] == 'Liftoff':
        adjust = consolidatedAdjust[
                # (consolidatedAdjust.part_month == custoApp['dataInicio'][index].month)
                # & (consolidatedAdjust.part_year == custoApp['dataInicio'][index].year)
                 (consolidatedAdjust.created_at >= custoApp['dataInicio'][index])
                & (consolidatedAdjust.os_name == custoApp['sistemaOperacional'][index])
                & (consolidatedAdjust.source == custoApp['origem'][index])
                | (consolidatedAdjust.source == 'Liftoff RE')
            ]
        # adjust = adjust[(consolidatedAdjust.created_at == custoApp['dataInicio'][index])]
    else:
        adjust = consolidatedAdjust[
                        # (consolidatedAdjust.part_month == custoApp['dataInicio'][index].month)
                        # & (consolidatedAdjust.part_year == custoApp['dataInicio'][index].year)
                        (consolidatedAdjust.created_at >= custoApp['dataInicio'][index])
                        & (consolidatedAdjust.os_name == custoApp['sistemaOperacional'][index])
                        & (consolidatedAdjust.source == custoApp['origem'][index])
                    ]

    if len(adjust) == 0:
        continue 

    
    #Estanciando a classe de manipulação de dados
    data = Data(dfAdjust=adjust,dfCustoApp=custoApp)
    #Teste lógico para calcular cada tipo de modelo
    if custoApp['modelo'][index] == 'CPM':
        adjust = data.cpm(index=index)
    elif custoApp['modelo'][index] == 'CPI':
        adjust = data.cpi(index=index)
    elif custoApp['modelo'][index] == 'CPA':
        adjust = data.cpa(index=index)
    #Apos fazer os calculos para cada tipo de modelo respectivo 
    #vamos consolidar o resultado em um data Frame que será retornado para um google sheets de return.
    results = results.append(adjust)

#Dessa maneira eu substituo os valores Nan de um dataFrame por zero.
results = results.fillna(0)
#dropar todas as linhas que sejam totalmente iguais
# results.drop_duplicates(keep=False,inplace=True)

#Vamos subir o data Frame results na planilha do google sheets
matrizParceiros.insertDataFrameToGsheets(workSheetName='resultsCustosApp_Atual',dataFrame=results,clear=True)
