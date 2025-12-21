import gspread
import pandas as pd
from datetime import datetime
import os
import json 
import sys 
from gspread.exceptions import WorksheetNotFound, APIError 
import numpy as np 

# --- Configurações ESPECÍFICAS DE GASTOS ---
ID_HISTORICO = "1DU3oxwCLCVmmYA9oD9lrGkBx2SyI87UtPw-BDDwA9EA" # ID da planilha de Histórico de Despesas
NOME_ABA_DADOS = "GASTOS"                               # Nome da aba de dados (MAIÚSCULO)
OUTPUT_HTML = "dashboard_historico_gastos.html"         # Nome do arquivo de saída
URL_DASHBOARD = "https://acmsilva1.github.io/analise-de-gastos/dashboard_historico_gastos.html" # URL de hospedagem (ajuste o caminho se necessário)

# Nomes de colunas (AJUSTADAS)
COLUNA_DATA = 'DATA E HORA'
COLUNA_VALOR = 'VALOR'         
COLUNA_ITEM = 'PRODUTO'        
COLUNA_QUANTIDADE = 'QUANTIDADE'

def autenticar_gspread():
    print("DEBUG: 1. Iniciando autenticação...")
    try:
        SHEET_CREDENTIALS_JSON = os.environ.get('GCP_SA_CREDENTIALS')
        
        if not SHEET_CREDENTIALS_JSON:
            gc = gspread.service_account(filename='credenciais.json')
            print("DEBUG: 1.2 Autenticação via arquivo local concluída com SUCESSO (Apenas para testes locais).")
            return gc
        
        credentials_dict = json.loads(SHEET_CREDENTIALS_JSON) 
        gc = gspread.service_account_from_dict(credentials_dict)
        print("DEBUG: 1.2 Autenticação via Secret concluída com SUCESSO.")
        return gc

    except Exception as e:
        detailed_error = f"FALHA CRÍTICA DE AUTENTICAÇÃO: Tipo: {type(e).__name__} | Mensagem: {e}"
        print(f"ERRO CRÍTICO DE AUTENTICAÇÃO DETALHADO: {detailed_error}")
        raise ConnectionError(detailed_error)


def gerar_analise_historica_gastos():
    try:
        gc = autenticar_gspread()
        planilha_historico = gc.open_by_key(ID_HISTORICO)
        
        try:
             aba_dados = planilha_historico.worksheet(NOME_ABA_DADOS)
        except WorksheetNotFound:
             raise WorksheetNotFound(f"A aba '{NOME_ABA_DADOS}' não foi encontrada. Verifique o nome.")
        
        print(f"DEBUG: 2.0 SUCESSO. ABA ENCONTRADA: '{aba_dados.title}'. Tentando obter valores...") 

        dados = aba_dados.get_all_values()

        if not dados or len(dados) < 2:
             raise ValueError("Planilha Vazia ou Cabeçalho Incompleto: Retornou menos de 2 linhas.")
        
        # 3. Processamento de Dados
        headers = dados[0]
        data = dados[1:]
        df = pd.DataFrame(data, columns=headers)
        
        # 3.1 Checagem de Colunas (Governanca de Dados)
        cols_check = [COLUNA_DATA, COLUNA_VALOR, COLUNA_ITEM, COLUNA_QUANTIDADE]
        if not all(col in df.columns for col in cols_check):
            missing_cols = [c for c in cols_check if c not in df.columns]
            raise ValueError(f"COLUNAS AUSENTES: A planilha não contém as colunas chave: {missing_cols}. Revise as variáveis de coluna no script.")

        # 4. Tratamento e Limpeza
        
        # 4.1 Limpeza do Valor (VALOR TOTAL DA COMPRA)
        df['temp_valor'] = df[COLUNA_VALOR].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=True).str.strip()
        df['Gasto_Total_Transacao'] = pd.to_numeric(df['temp_valor'], errors='coerce') # Usando o VALOR LIMPO como GASTO TOTAL

        # 4.2 Limpeza da Quantidade (para contagem de itens)
        df['Quantidade_Float'] = pd.to_numeric(df[COLUNA_QUANTIDADE], errors='coerce')
        
        # 4.3 Limpeza da Data
        df['Data_Datetime'] = pd.to_datetime(df[COLUNA_DATA], errors='coerce', dayfirst=True)
        
        # 4.4 Filtragem: Apenas linhas onde Gasto Total e Data são válidos
        df_validos = df.dropna(subset=['Data_Datetime', 'Gasto_Total_Transacao', 'Quantidade_Float']).copy()
        
        # 4.5 Checagem Final
        if df_validos.empty:
             raise ValueError("Nenhum dado válido encontrado após a limpeza. Planilha contém apenas sujeira ou colunas incorretas.")
        
        print(f"DEBUG: 4.5 {len(df_validos)} linhas válidas prontas para análise.")

        # 5. Análise e Tendências
        
        ano_atual = datetime.now().year
        df_validos['Mes_Ano'] = df_validos['Data_Datetime'].dt.to_period('M')
        
        # 5.1 Agrupamento MENSAL (Soma do Gasto Total e Contagem de Itens)
        gastos_mensais = df_validos.groupby('Mes_Ano').agg(
            {'Gasto_Total_Transacao': 'sum',
             'Quantidade_Float': 'sum'  # Soma a quantidade total de itens comprados
            }
        ).rename(columns={'Quantidade_Float': 'Total_Itens_Comprados'}).reset_index()

        gastos_mensais['Mes_Ano'] = gastos_mensais['Mes_Ano'].astype(str)

        # 5.2 Cálculo da Variação Mensal (MoM)
        gastos_mensais['Gastos_Anteriores'] = gastos_mensais['Gasto_Total_Transacao'].shift(1)
        # Cálculo da Variação
        gastos_mensais['Variacao_Mensal'] = (
            (gastos_mensais['Gasto_Total_Transacao'] - gastos_mensais['Gastos_Anteriores']) / np.where(gastos_mensais['Gastos_Anteriores'] == 0, np.nan, gastos_mensais['Gastos_Anteriores'])
        ) * 100
        
        # 5.3 Análise de Itens de Despesa (Top 10 Produtos por Gasto)
        gastos_por_item = df_validos.groupby(COLUNA_ITEM)['Gasto_Total_Transacao'].sum().sort_values(ascending=False).head(10)
        
        # --- FILTRO DE GOVERNANÇA: FILTRAGEM PARA O DASHBOARD (YTD) ---
        gastos_mensais_ytd = gastos_mensais[
            gastos_mensais['Mes_Ano'].str.startswith(str(ano_atual)) 
        ].copy() 

        # 5.4 Cálculo dos Totais YTD
        total_gastos_ytd = gastos_mensais_ytd['Gasto_Total_Transacao'].sum()
        total_itens_ytd = gastos_mensais_ytd['Total_Itens_Comprados'].sum()
        
        # O insight de tendência (focado na variação do gasto)
        if not gastos_mensais.empty:
            ultimo_mes = gastos_mensais.iloc[-1]
            tendencia = ultimo_mes['Variacao_Mensal']
            
            if pd.isna(tendencia):
                insight_tendencia = "Início da análise. Ainda não há tendência Mês-a-Mês."
            elif tendencia > 5:
                insight_tendencia = f"⚠️ Forte aumento de custo de {tendencia:.2f}% no último mês! Investigar!"
            elif tendencia > 0:
                insight_tendencia = f"📈 Gasto em crescimento. Aumento de {tendencia:.2f}%."
            else:
                insight_tendencia = f"🎉 Redução de custos de {-tendencia:.2f}%! Parabéns." 
        else:
            insight_tendencia = "Nenhum dado válido encontrado para análise de tendências."

        # 6. Geração da Tabela HTML Mensal (Métricas Solicitadas)
        table_rows_mensal = ""
        for index, row in gastos_mensais_ytd.iterrows():
            
            # Convenção de Cores: Gasto POSITIVO (ruim) é vermelho/negativo, Gasto NEGATIVO (bom) é verde/positivo
            variacao_display = f'<td class="val-col"><span class="{"negativo" if row["Variacao_Mensal"] > 0 else "positivo"}">{row["Variacao_Mensal"]:.2f}%</span></td>' if pd.notna(row["Variacao_Mensal"]) else '<td class="val-col">N/A</td>'
            gasto_display = f'<td class="val-col">R$ {row["Gasto_Total_Transacao"]:,.2f}</td>'
            contagem_display = f'<td class="val-col">{int(row["Total_Itens_Comprados"])}</td>'
            
            table_rows_mensal += f"<tr><td>{row['Mes_Ano']}</td>{gasto_display}{contagem_display}{variacao_display}</tr>\n"
            
        # 6.1 Geração da Tabela Top Itens (Itens e Gasto Total)
        table_rows_itens = ""
        for item, gasto in gastos_por_item.items():
            table_rows_itens += f"<tr><td>{item}</td><td>R$ {gasto:,.2f}</td></tr>\n"


        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Dashboard Histórico de Gastos - Tendências</title>
             <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f4f7f6; color: #333; }}
                .container {{ max-width: 900px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
                h2 {{ color: #dc3545; border-bottom: 2px solid #dc3545; padding-bottom: 10px; }}
                .metric-box {{ padding: 15px; margin-bottom: 15px; border-radius: 6px; }}
                .insight {{ background-color: #f8d7da; border-left: 5px solid #dc3545; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
                th, td {{ padding: 12px; border: 1px solid #ddd; text-align: left; }}
                th {{ background-color: #dc3545; color: white; }}
                .positivo {{ color: green; font-weight: bold; }} /* Usado para indicar redução de custo */
                .negativo {{ color: red; font-weight: bold; }}  /* Usado para indicar aumento de custo */
            </style>
        </head>
        <body>
            <div class="container">
                <h2>💸 Análise Histórica e Tendências de GASTOS ({ano_atual} YTD Total: R$ {total_gastos_ytd:,.2f} | {int(total_itens_ytd)} Itens Comprados)</h2>
                <p>Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} (Lendo {len(df_validos)} registros válidos)</p>
                
                <div class="metric-box insight">
                    <h3>Insights de Tendência de Custo</h3>
                    <p>{insight_tendencia}</p>
                </div>

                <h2>📈 Gastos Consolidados Mês a Mês</h2>
                <table class="table">
                    <thead>
                        <tr>
                            <th>Mês/Ano</th>
                            <th>Total de Gastos</th>
                            <th>Total de Itens Comprados</th> 
                            <th>Tendência Mensal (Var. %)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows_mensal}
                    </tbody>
                </table>

                <h2>🧾 Top 10 Itens/Categorias por Gasto (YTD)</h2>
                <table class="table">
                    <thead>
                        <tr>
                            <th>Item/Produto</th>
                            <th>Gasto Total (R$)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows_itens}
                    </tbody>
                </table>

                <p style="margin-top: 20px; font-size: 0.9em; color: #777;">Dashboard hospedado em: <a href="{URL_DASHBOARD}" target="_blank">{URL_DASHBOARD}</a></p>
                
            </div>
        </body>
        </html>
        """
        
        # Bloco de escrita
        try:
            with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            print(f"DEBUG: 5.0 Escrita do HTML concluída. Arquivo: {OUTPUT_HTML}")

        except IOError as io_e:
            raise IOError(f"Falha na escrita do arquivo HTML no disco: {io_e}")
            
        print(f"Análise Histórica de Gastos concluída! {OUTPUT_HTML} gerado com sucesso.")

    except (APIError, WorksheetNotFound, ValueError, ConnectionError, Exception) as e:
        error_message = str(e) if str(e) else f"ERRO INDEFINIDO. Tipo de erro: {type(e).__name__}."
        
        print(f"ERRO DE EXECUÇÃO FINAL: {error_message}")
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
             f.write(f"<html><body><h2>Erro Crítico na Geração do Dashboard Histórico de Gastos</h2><p>Detalhes: {error_message}</p></body></html>")
        
if __name__ == "__main__":
    gerar_analise_historica_gastos()
