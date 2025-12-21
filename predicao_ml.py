import gspread
import pandas as pd
import numpy as np
from datetime import datetime
import os
import json 
from gspread.exceptions import WorksheetNotFound, APIError 

# --- Adicionando as bibliotecas de Machine Learning ---
from sklearn.linear_model import LinearRegression 
from sklearn.metrics import mean_absolute_error

# --- CONFIGURAÇÕES DE DADOS HISTÓRICOS (VENDAS E GASTOS) ---
# VENDAS
ID_HISTORICO_VENDAS = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y"
ABA_VENDAS = "VENDAS"
COLUNA_VALOR_VENDA = 'VALOR DA VENDA'

# GASTOS
ID_HISTORICO_GASTOS = "1DU3oxwCLCVmmYA9oD9lrGkBx2SyI87UtPw-BDDwA9EA"
ABA_GASTOS = "GASTOS"
COLUNA_VALOR_GASTO = 'VALOR' # Coluna 'VALOR' na planilha de Gastos
COLUNA_DATA = 'DATA E HORA' # Coluna de Data é a mesma em ambas

OUTPUT_HTML = "dashboard_ml_insights.html"
URL_DASHBOARD = "https://acmsilva1.github.io/analise-de-vendas/dashboard_ml_insights.html" 
# --------------------------------------------------------------------------------

def autenticar_gspread():
    SHEET_CREDENTIALS_JSON = os.environ.get('GCP_SA_CREDENTIALS')
    if not SHEET_CREDENTIALS_JSON:
        raise ConnectionError("Variável de ambiente 'GCP_SA_CREDENTIALS' não encontrada.")
    credentials_dict = json.loads(SHEET_CREDENTIALS_JSON) 
    return gspread.service_account_from_dict(credentials_dict)

def carregar_dados_de_planilha(gc, sheet_id, aba_nome, coluna_valor, prefixo):
    print(f"DEBUG: Carregando dados: ID={sheet_id}, Aba={aba_nome}")
    try:
        planilha = gc.open_by_key(sheet_id).worksheet(aba_nome)
        dados = planilha.get_all_values()
        
        if not dados or len(dados) < 2:
             print(f"Alerta: Planilha {aba_nome} está vazia.")
             return pd.DataFrame()
             
        df = pd.DataFrame(dados[1:], columns=dados[0])
        
        # Limpeza do Valor (R$ e substitui vírgula por ponto)
        df['temp_valor'] = df[coluna_valor].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=True).str.strip()
        df[f'{prefixo}_Float'] = pd.to_numeric(df['temp_valor'], errors='coerce')
        
        # Limpeza da Data
        df['Data_Datetime'] = pd.to_datetime(df[COLUNA_DATA], errors='coerce', dayfirst=True)
        
        # Agrupamento Mensal
        df_validos = df.dropna(subset=['Data_Datetime', f'{prefixo}_Float']).copy()
        df_validos['Mes_Ano'] = df_validos['Data_Datetime'].dt.to_period('M')
        
        df_mensal = df_validos.groupby('Mes_Ano')[f'{prefixo}_Float'].sum().reset_index()
        df_mensal.columns = ['Mes_Ano', f'Total_{prefixo}']
        
        return df_mensal.set_index('Mes_Ano')
        
    except Exception as e:
        print(f"ERRO ao carregar {aba_nome}: {e}")
        return pd.DataFrame()

def carregar_e_combinar_dados(gc):
    # 1. Carregar Vendas e Gastos separadamente
    df_vendas = carregar_dados_de_planilha(gc, ID_HISTORICO_VENDAS, ABA_VENDAS, COLUNA_VALOR_VENDA, 'Vendas')
    df_gastos = carregar_dados_de_planilha(gc, ID_HISTORICO_GASTOS, ABA_GASTOS, COLUNA_VALOR_GASTO, 'Gastos')
    
    if df_vendas.empty or df_gastos.empty:
        raise ValueError("Dados insuficientes para análise de Lucro (Vendas ou Gastos estão vazios).")

    # 2. Combinar os DataFrames pelo Índice (Mês_Ano)
    df_combinado = pd.merge(
        df_vendas, 
        df_gastos, 
        left_index=True, 
        right_index=True, 
        how='outer' # Garante que todos os meses com alguma transação sejam incluídos
    ).fillna(0) # Preenche meses onde não houve venda ou gasto com 0

    # 3. Calcular o Lucro Líquido
    df_combinado['Lucro_Liquido'] = df_combinado['Total_Vendas'] - df_combinado['Total_Gastos']
    
    # 4. Adicionar Feature Numérica (Mes_Index)
    df_combinado = df_combinado.sort_index().reset_index()
    df_combinado['Mes_Index'] = np.arange(len(df_combinado))
    
    if len(df_combinado) < 4:
        raise ValueError(f"Dados insuficientes para ML: Apenas {len(df_combinado)} meses consolidados. Mínimo de 4 meses é recomendado.")
            
    return df_combinado

def treinar_e_prever(df_mensal):
    # O modelo agora prevê o Lucro Líquido
    X = df_mensal[['Mes_Index']] 
    y = df_mensal['Lucro_Liquido'] 
    
    # Treinamento do Modelo
    modelo = LinearRegression()
    modelo.fit(X, y)
    
    # Previsão para o Próximo Mês
    proximo_mes_index = df_mensal['Mes_Index'].max() + 1
    previsao_proximo_mes = modelo.predict([[proximo_mes_index]])[0]

    # Cálculo da Métrica de Erro
    predicoes_historicas = modelo.predict(X)
    mae = mean_absolute_error(y, predicoes_historicas)

    # Último Valor para Comparação
    ultimo_lucro_real = df_mensal['Lucro_Liquido'].iloc[-1]
    
    return previsao_proximo_mes, mae, ultimo_lucro_real

def montar_dashboard_ml(previsao, mae, ultimo_valor_real):
    
    # Lógica de Classificação do Insight (Focada no Lucro Líquido)
    diferenca = previsao - ultimo_valor_real
    
    if previsao < 0:
        insight = f"🚨 **Previsão de PREJUÍZO!** Lucro negativo de {abs(diferenca):,.2f} esperado."
        cor = "#dc3545" # Vermelho
    elif diferenca > (ultimo_valor_real * 0.10):
        insight = f"🚀 **Crescimento de Lucro Esperado!** Aumento de {diferenca:,.2f}."
        cor = "#28a745" # Verde
    elif diferenca < -(ultimo_valor_real * 0.10):
        insight = f"⚠️ **Risco de Queda de Lucro!** Retração de {abs(diferenca):,.2f} esperada. Analise seus custos!"
        cor = "#ffc107" # Amarelo
    else:
        insight = f"➡️ **Estabilidade Esperada.** Lucro projetado próximo ao mês passado."
        cor = "#17a2b8" # Azul Claro

    def format_brl(value):
        return f"R$ {value:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ',')

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Dashboard ML Insights - Previsão de Lucro Líquido</title>
         <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f4f7f6; color: #333; }}
            .container {{ max-width: 900px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
            h2 {{ color: #6f42c1; border-bottom: 2px solid #6f42c1; padding-bottom: 10px; }}
            .metric-box {{ padding: 20px; margin-bottom: 20px; border-radius: 8px; background-color: {cor}; color: white; text-align: center; }}
            .metric-box h3 {{ margin-top: 0; font-size: 1.5em; }}
            .metric-box p {{ font-size: 2.5em; font-weight: bold; }}
            .info-box {{ padding: 10px; border: 1px dashed #ccc; background-color: #f8f9fa; margin-top: 15px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>🔮 Insights de Machine Learning: Previsão de LUCRO LÍQUIDO</h2>
            <p>Modelo: Regressão Linear Simples (scikit-learn)</p>
            <p>Data da Previsão: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
            
            <div class="metric-box">
                <h3>Lucro Líquido Projetado para o Próximo Mês</h3>
                <p>{format_brl(previsao)}</p>
            </div>
            
            <div class="info-box">
                <h4>Insight da Previsão:</h4>
                <p>{insight}</p>
            </div>

            <div class="info-box">
                <h4>Métricas de Qualidade (Governança de IA)</h4>
                <p>Lucro Real Mês Passado: **{format_brl(ultimo_valor_real)}**</p>
                <p>Erro Absoluto Médio Histórico (MAE): **{format_brl(mae)}**</p>
                <p>*(O MAE representa a margem de erro histórica do modelo na predição do Lucro Líquido.)</p>
            </div>
            
            <p style="margin-top: 20px; font-size: 0.9em; color: #777;">Dashboard hospedado em: <a href="{URL_DASHBOARD}" target="_blank">{URL_DASHBOARD}</a></p>
        </div>
    </body>
    </html>
    """
    
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"Dashboard de ML gerado com sucesso: {OUTPUT_HTML}")


# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    try:
        gc = autenticar_gspread()
        df_mensal = carregar_e_combinar_dados(gc)
        
        if not df_mensal.empty:
            previsao, mae, ultimo_lucro_real = treinar_e_prever(df_mensal)
            montar_dashboard_ml(previsao, mae, ultimo_lucro_real)
        else:
            print("Execução ML interrompida por falta de dados históricos.")
            
    except Exception as e:
        error_message = str(e)
        print(f"ERRO CRÍTICO NA EXECUÇÃO DO ML: {error_message}")
        # Criar um arquivo HTML de erro para governança
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
             f.write(f"<html><body><h2>Erro Crítico na Geração do ML Dashboard</h2><p>Detalhes: {error_message}</p></body></html>")
