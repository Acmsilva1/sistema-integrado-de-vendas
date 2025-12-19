import pandas as pd
import gspread
import os
import plotly.express as px
from datetime import datetime
from io import StringIO
import locale
import json # <--- NOVO IMPORT AQUI

# Configuração de localização para formatação monetária (Ajuste se necessário)
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR')
    except locale.Error:
        pass # Ignora o erro se a localização não puder ser configurada

# --- 1. CONFIGURAÇÕES E AUTENTICAÇÃO ---
try:
    SHEET_CREDENTIALS_JSON = os.environ.get('GCP_SA_CREDENTIALS')
    
    # 🚨 CORREÇÃO CRÍTICA AQUI: Usar json.loads para transformar a string em dict
    credentials_dict = json.loads(SHEET_CREDENTIALS_JSON) 
    gc = gspread.service_account_from_dict(credentials_dict)
    
except Exception as e:
    # Este bloco só será atingido se a credencial JSON for inválida
    # ou se estiver rodando localmente sem o arquivo.
    print(f"ERRO DE AUTENTICAÇÃO: {e}")
    # O GITHUB ACTIONS NÃO VAI ENCONTRAR O ARQUIVO LOCAL, ENTÃO AQUI VAI FALHAR SE O TRY FALHAR
    gc = gspread.service_account() # Mantido para fins de depuração local

SPREADSHEET_ID = "1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug"
WORKSHEET_NAME = "vendas"

# --- 2. FUNÇÃO DE CARREGAMENTO E LIMPEZA DE DADOS ---
def carregar_e_limpar_dados():
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.worksheet(WORKSHEET_NAME)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    # Limpeza da Coluna 'Total Venda' e conversão para float
    df['Total Venda'] = (
        df['Total Venda']
        .astype(str)
        .str.replace('R$', '', regex=False)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
        .str.strip()
    )
    df['Total Venda'] = pd.to_numeric(df['Total Venda'], errors='coerce')
    df.dropna(subset=['Total Venda'], inplace=True)

    # Conversão da Coluna de Data/Hora
    df['Data/Hora Venda'] = pd.to_datetime(df['Data/Hora Venda'], errors='coerce')
    df.dropna(subset=['Data/Hora Venda'], inplace=True)
    df['Hora'] = df['Data/Hora Venda'].dt.hour

    return df

# --- 3. ANÁLISES E MONTAGEM DO HTML ---
def criar_dashboard_html(df):
    # --- 3.1. CÁLCULOS DOS KPIS ---
    total_vendas = df['Total Venda'].sum()
    sabor_mais_vendido = df['Item'].mode()[0]
    
    melhor_cliente_df = df.groupby('Cliente')['Total Venda'].sum().sort_values(ascending=False)
    melhor_cliente = melhor_cliente_df.index[0]
    melhor_cliente_gasto = melhor_cliente_df.iloc[0]
    
    pico_hora_df = df['Hora'].value_counts()
    pico_hora = pico_hora_df.index[0]
    
    # Formatação de Moeda
    total_vendas_fmt = locale.currency(total_vendas, grouping=True)
    melhor_cliente_gasto_fmt = locale.currency(melhor_cliente_gasto, grouping=True)

    # --- 3.2. VISUALIZAÇÕES COM PLOTLY ---
    
    # Gráfico 1: Vendas por Sabor/Item
    vendas_por_item = df['Item'].value_counts().reset_index()
    vendas_por_item.columns = ['Item', 'Contagem']
    fig_sabor = px.bar(
        vendas_por_item.head(10).sort_values(by='Contagem'), 
        x='Contagem', y='Item', 
        orientation='h', 
        title='Top 10 Sabores/Itens Mais Vendidos (Contagem)',
        template='plotly_dark'
    )
    fig_sabor.update_layout(autosize=True, height=500, margin=dict(l=10, r=10, t=40, b=10))

    # Gráfico 2: Pico de Vendas por Hora do Dia
    fig_hora = px.bar(
        pico_hora_df.reset_index(), 
        x='Hora', y='count', 
        title='Frequência de Vendas por Hora (Pico: ' + str(pico_hora) + 'h)',
        template='plotly_dark'
    )
    fig_hora.update_xaxes(tick0=0, dtick=1)
    fig_hora.update_layout(autosize=True, height=500, margin=dict(l=10, r=10, t=40, b=10))
    
    # Gráfico 3: Melhores Clientes por Gasto Total
    fig_cliente = px.bar(
        melhor_cliente_df.head(5).reset_index().rename(columns={'Total Venda': 'Gasto Total'}), 
        x='Cliente', y='Gasto Total', 
        title='Top 5 Clientes por Gasto Total',
        template='plotly_dark'
    )
    fig_cliente.update_layout(autosize=True, height=500, margin=dict(l=10, r=10, t=40, b=10))

    # --- 3.3. MONTAGEM FINAL DO HTML COM LAYOUT RESPONSIVO ---
    
    # Estilos CSS Inclusos para Responsividade (Flexbox)
    styles = """
    <style>
        body { font-family: Arial, sans-serif; background-color: #1e1e1e; color: white; margin: 0; padding: 10px; }
        .kpi-container { display: flex; flex-wrap: wrap; justify-content: space-around; background-color: #2e2e2e; padding: 15px; border-radius: 10px; margin-bottom: 20px; }
        .kpi-box { text-align: center; padding: 10px; min-width: 200px; flex: 1; margin: 5px; }
        .kpi-box h2 { font-size: 1.1em; margin-bottom: 5px; }
        .kpi-box p { font-size: 1.6em; font-weight: bold; margin-top: 5px; }
        .chart-container { display: flex; flex-wrap: wrap; justify-content: space-between; }
        .chart-item { width: 100%; margin-bottom: 20px; } /* 100% no mobile */
        @media (min-width: 768px) {
            .chart-item { width: 48%; } /* 48% em telas maiores (desktop) */
        }
    </style>
    """

    kpi_html = f"""
    <div class="kpi-container">
        <div class="kpi-box">
            <h2 style="color: #64ffda;">Total Arrecadado</h2>
            <p style="color: #64ffda;">{total_vendas_fmt}</p>
        </div>
        <div class="kpi-box">
            <h2 style="color: #2196F3;">Sabor Campeão</h2>
            <p style="color: #2196F3;">{sabor_mais_vendido}</p>
        </div>
        <div class="kpi-box">
            <h2 style="color: #FF9800;">Pico de Vendas</h2>
            <p style="color: #FF9800;">{pico_hora}h</p>
        </div>
        <div class="kpi-box">
            <h2 style="color: #E91E63;">Melhor Cliente</h2>
            <p style="color: #E91E63;">{melhor_cliente} ({melhor_cliente_gasto_fmt})</p>
        </div>
    </div>
    """

    # Combina tudo no HTML final
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Dashboard Detalhado de Vendas</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0"> 
        {styles}
    </head>
    <body>
        <h1 style="text-align: center; color: #64ffda;">Dashboard Detalhado de Vendas</h1>
        <p style="text-align: center; color: #aaa;">Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
        
        {kpi_html}
        
        <div class="chart-container">
            <div class="chart-item">
                {fig_sabor.to_html(full_html=False, include_plotlyjs='cdn')}
            </div>
            <div class="chart-item">
                {fig_hora.to_html(full_html=False, include_plotlyjs='cdn')}
            </div>
             <div class="chart-item" style="width: 100%;">
                {fig_cliente.to_html(full_html=False, include_plotlyjs='cdn')}
            </div>
        </div>
        
    </body>
    </html>
    """
    
    return html_content

# --- 4. EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    try:
        df_vendas = carregar_e_limpar_dados()
        final_html = criar_dashboard_html(df_vendas)

        with open("dashboard_vendas_final.html", "w") as f:
            f.write(final_html)

        print("Dashboard HTML detalhado e otimizado para responsividade (via CSS) foi gerado com sucesso.")

    except Exception as e:
        print(f"Ocorreu um erro no script de automação: {e}")
        exit(1)

