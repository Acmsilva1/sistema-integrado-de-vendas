import gspread
import pandas as pd
from datetime import datetime
import os
import json 
import sys # Mantido para debug

# --- Configurações ---
ID_HISTORICO = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y"
OUTPUT_HTML = "dashboard_historico.html"
URL_DASHBOARD = "https://acmsilva1.github.io/analise-de-vendas/dashboard_historico.html"
COLUNA_DATA = 'DATA E HORA'
COLUNA_VALOR = 'VALOR DA VENDA'

def autenticar_gspread():
    print("DEBUG: 1. Iniciando autenticação...")
    try:
        SHEET_CREDENTIALS_JSON = os.environ.get('GCP_SA_CREDENTIALS')
        
        if not SHEET_CREDENTIALS_JSON:
            # Ponto de falha mais comum: Secret não está sendo injetado.
            print("DEBUG: 1.1 FALHA: Variável GCP_SA_CREDENTIALS está VAZIA ou não foi injetada. Tentando credenciais.json.")
            # Tentativa local (que vai falhar no CI/CD)
            gc = gspread.service_account(filename='credenciais.json')
            print("DEBUG: 1.2 Autenticação via arquivo local concluída com SUCESSO (Apenas se o arquivo existir).")
            return gc
        
        # Ponto de falha 2: Secret injetado, mas com falha no JSON.
        print(f"DEBUG: 1.1 SUCESSO: Secret encontrado. Tentando json.loads... (Tamanho: {len(SHEET_CREDENTIALS_JSON)})")
        credentials_dict = json.loads(SHEET_CREDENTIALS_JSON) 
        gc = gspread.service_account_from_dict(credentials_dict)
        print("DEBUG: 1.2 Autenticação via Secret concluída com SUCESSO.")
        return gc

    except Exception as e:
        # 🚨 FORÇANDO LOG DETALHADO ANTES DE PROPAGAR O ERRO
        detailed_error = f"FALHA CRÍTICA DE AUTENTICAÇÃO: Tipo: {type(e).__name__} | Mensagem: {e}"
        print(f"ERRO CRÍTICO DE AUTENTICAÇÃO DETALHADO: {detailed_error}")
        # Re-lança a exceção com a mensagem detalhada
        raise ConnectionError(detailed_error)


def gerar_analise_historica():
    total_vendas_global = 0
    
    try:
        # 1. Autenticação
        gc = autenticar_gspread()
        
        # Ponto de falha 3: Planilha ou acesso negado.
        print("DEBUG: 2. Tentando abrir a planilha com ID: " + ID_HISTORICO)
        planilha_historico = gc.open_by_key(ID_HISTORICO).worksheet(0)
        
        dados = planilha_historico.get_all_values()
        headers = dados[0]
        data = dados[1:]
        
        # ... (Resto da lógica de processamento e HTML) ...
        # (Use o restante da Versão 3.0 que contém a lógica de análise)

        df = pd.DataFrame(data, columns=headers)
        df[COLUNA_VALOR] = df[COLUNA_VALOR].astype(str).str.replace(',', '.', regex=True)
        df['Valor_Venda_Float'] = pd.to_numeric(df[COLUNA_VALOR], errors='coerce')
        df['Data_Datetime'] = pd.to_datetime(df[COLUNA_DATA], errors='coerce', dayfirst=True)
        df_validos = df.dropna(subset=['Data_Datetime', 'Valor_Venda_Float']).copy()

        vendas_mensais = ... # Recalcule vendas_mensais
        total_vendas_global = ... # Recalcule total_vendas_global
        insight_tendencia = ... # Recalcule insight_tendencia
        table_rows = ... # Recalcule table_rows

        print(f"DEBUG: 3. Planilha lida e processada. {len(df_validos)} linhas válidas.")

        # Geração do HTML (Correto)
        html_content = f"""
        """
        
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Análise Histórica concluída! {OUTPUT_HTML} gerado com sucesso.")

    except Exception as e:
        # 🚨 Se a mensagem ainda for vazia, mostra o tipo de erro e reitera a necessidade do LOG.
        error_message = str(e) if str(e) else f"ERRO CRÍTICO SEM MENSAGEM: Falha na autenticação (Secret JSON) ou na leitura da Planilha. Tipo de erro: {type(e).__name__}. REVISE O LOG DO GITHUB ACTIONS POR LINHAS 'DEBUG:' e 'FALHA CRÍTICA DE AUTENTICAÇÃO'."
        
        print(f"ERRO DE EXECUÇÃO FINAL: {error_message}")
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
             f.write(f"<html><body><h2>Erro Crítico na Geração do Dashboard Histórico</h2><p>Detalhes: {error_message}</p></body></html>")
        
if __name__ == "__main__":
    gerar_analise_historica()
