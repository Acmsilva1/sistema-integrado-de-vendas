import pandas as pd
import gspread
import os
import plotly.express as px
from datetime import datetime
from io import StringIO
import json 

# --- FUNÇÃO HELPER PARA FORMATAR BRL (NÃO DEPENDE DO SISTEMA) ---
def format_brl(value):
    # Formata para R$ X.XXX,XX
    return f"R$ {value:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ',')

# --- 1. CONFIGURAÇÕES E AUTENTICAÇÃO ---
try:
    SHEET_CREDENTIALS_JSON = os.environ.get('GCP_SA_CREDENTIALS')
    
    credentials_dict = json.loads(SHEET_CREDENTIALS_JSON) 
    gc = gspread.service_account_from_dict(credentials_dict)
    
except Exception as e:
    print(f"ERRO DE AUTENTICAÇÃO: {e}")
    gc = gspread.service_account() 

SPREADSHEET_ID = "1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug"
WORKSHEET_NAME = "vendas"

# --- 2. FUNÇÃO DE CARREGAMENTO, LIMPEZA E FILTRAGEM ---
def carregar_e_limpar_dados():
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.worksheet(WORKSHEET_NAME)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    
    # 2.1. Limpeza da Coluna 'VALOR DA VENDA' e criação de 'Total Limpo'
    df['Total Limpo'] = (
        df['VALOR DA VENDA'] 
        .astype(str)
        .str.replace('R$', '', regex=False)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
        .str.strip()
    )
    df['Total Limpo'] = pd.to_numeric(df['Total Limpo'], errors='coerce')
    df.dropna(subset=['Total Limpo'], inplace=True)

    # 2.2. Conversão da Coluna de Data/Hora (Corrigida: DD/MM/AAAA HH:MM:SS)
    df['Data/Hora Venda'] = pd.to_datetime(df['DATA E HORA'], errors='coerce', format='%d/%m/%Y %H:%M:%S')
    df.dropna(subset=['Data/Hora Venda'], inplace=True)
    df['Hora'] = df['Data/Hora Venda'].dt.hour
    
    # 2.3. VERIFICAÇÃO DE INTEGRIDADE GERAL
    if df.empty:
        raise ValueError("O DataFrame está vazio após a limpeza de datas/valores. Sem dados para análise.")

    # 2.4. FILTRAGEM TEMPORAL PARA O DASHBOARD
    data_atual = datetime.now().date()
    
    df_dia_atual = df[df['Data/Hora Venda'].dt.date == data_atual].copy()
    
    mes_atual = data_atual.month
    ano_atual = data_atual.year
    df_mes_atual = df[(df['Data/Hora Venda'].dt.month == mes_atual) & (df['Data/Hora Venda'].dt.year == ano_atual)].copy()

    return df, df_mes_atual, df_dia_atual

# --- FUNÇÃO HELPER PARA CÁLCULOS ROBUSTOS (AGORA INCLUI CONTAGEM) ---
def calcular_kpis(df, periodo="Dia"):
    if df.empty:
        return {
            'total': 0.0,
            'total_fmt': format_brl(0.0),
            'contagem': 0, # NOVO: Contagem de Vendas
            'sabor': f'Sem Vendas ({periodo})',
            'cliente': f'N/A ({periodo})',
            'cliente_gasto_fmt': format_brl(0.0),
            'pico_hora': 'N/A'
        }
    
    total_vendas = df['Total Limpo'].sum()
    contagem = len(df) # NOVO: Contagem de Vendas
    sabor_mais_vendido = df['SABORES'].mode().iloc[0] if not df['SABORES'].empty else f'N/A ({periodo})'
    
    melhor_cliente_df = df.groupby('DADOS DO COMPRADOR')['Total Limpo'].sum().sort_values(ascending=False)
    melhor_cliente = melhor_cliente_df.index[0]
    melhor_cliente_gasto = melhor_cliente_df.iloc[0]
    
    pico_hora_df = df['Hora'].value_counts()
    pico_hora = pico_hora_df.index[0] if not pico_hora_df.empty else 'N/A'
    
    return {
        'total': total_vendas,
        'total_fmt': format_brl(total_vendas),
        'contagem': contagem, # NOVO: Contagem de Vendas
        'sabor': sabor_mais_vendido,
        'cliente': melhor_cliente,
        'cliente_gasto_fmt': format_brl(melhor_cliente_gasto),
        'pico_hora': pico_hora
    }

# --- 3. ANÁLISES E MONTAGEM DO HTML (MULTICAMADAS) ---
def criar_dashboard_html(df_completo, df_mes, df_dia):
    
    # --- 3.1. CÁLCULOS DOS KPIS POR CAMADA ---
    kpis_mes = calcular_kpis(df_mes, periodo="Mês")
    kpis_dia = calcular_kpis(df_dia, periodo="Dia")
    
    # --- 3.2. VISUALIZAÇÕES COM PLOTLY ---
    # Usando df_mes para contexto
    vendas_por_item = df_mes['SABORES'].value_counts().reset_index() 
    vendas_por_item.columns = ['Item', 'Contagem']
    fig_sabor = px.bar(
        vendas_por_item.head(10).sort_values(by='Contagem', ascending=True), 
        x='Contagem', y='Item', 
        orientation='h', 
        title='Top 10 Sabores/Itens Mais Vendidos (Mês Atual)',
        template='plotly_dark'
    )
    fig_sabor.update_layout(autosize=True, height=500, margin=dict(l=10, r=10, t=40, b=10))

    # Usando df_dia para foco imediato
    pico_hora_df_dia = df_dia['Hora'].value_counts()
    fig_hora = px.bar(
        pico_hora_df_dia.reset_index(), 
        x='Hora', y='count', 
        title=f'Frequência de Vendas por Hora (Hoje) - Pico: {kpis_dia["pico_hora"]}h',
        template='plotly_dark'
    )
    fig_hora.update_xaxes(tick0=0, dtick=1)
    fig_hora.update_layout(autosize=True, height=500, margin=dict(l=10, r=10, t=40, b=10))
    
    # Melhores Clientes por Gasto Total (Mensal)
    melhor_cliente_df_mes = df_mes.groupby('DADOS DO COMPRADOR')['Total Limpo'].sum().sort_values(ascending=False)
    fig_cliente = px.bar(
        melhor_cliente_df_mes.head(5).reset_index().rename(columns={'Total Limpo': 'Gasto Total'}),
        x='DADOS DO COMPRADOR', y='Gasto Total', 
        title='Top 5 Clientes por Gasto Total (Mês Atual)',
        template='plotly_dark'
    )
    fig_cliente.update_layout(autosize=True, height=500, margin=dict(l=10, r=10, t=40, b=10))

    # --- 3.3. MONTAGEM FINAL DO HTML ---
    
    styles = """
    <style>
        body { font-family: Arial, sans-serif; background-color: #1e1e1e; color: white; margin: 0; padding: 10px; }
        .header-section { margin-top: 30px; padding-bottom: 10px; border-bottom: 2px solid #64ffda; }
        .kpi-container { display: flex; flex-wrap: wrap; justify-content: space-around; background-color: #2e2e2e; padding: 15px; border-radius: 10px; margin-bottom: 20px; }
        .kpi-box { text-align: center; padding: 10px; min-width: 200px; flex: 1; margin: 5px; }
        .kpi-box h2 { font-size: 1.1em; margin-bottom: 5px; }
        .kpi-box p { font-size: 1.6em; font-weight: bold; margin-top: 5px; }
        .chart-container { display: flex; flex-wrap: wrap; justify-content: space-between; }
        .chart-item { width: 100%; margin-bottom: 20px; } 
        @media (min-width: 768px) {
            .chart-item { width: 48%; } 
        }
    </style>
    """

    # Blocos KPI Mensal - Adicionando Contagem
    kpi_mes_html = f"""
    <div class="kpi-container">
        <div class="kpi-box">
            <h2 style="color: #64ffda;">Total Arrecadado (MÊS)</h2>
            <p style="color: #64ffda;">{kpis_mes['total_fmt']}</p>
        </div>
        <div class="kpi-box">
            <h2 style="color: #2196F3;">Contagem de Vendas (MÊS)</h2>
            <p style="color: #2196F3;">{kpis_mes['contagem']}</p>
        </div>
        <div class="kpi-box">
            <h2 style="color: #E91E63;">Melhor Cliente (MÊS)</h2>
            <p style="color: #E91E63;">{kpis_mes['cliente']} ({kpis_mes['cliente_gasto_fmt']})</p>
        </div>
    </div>
    """
    
    # Blocos KPI Diário - Adicionando Contagem
    kpi_dia_html = f"""
    <div class="kpi-container" style="background-color: #383838;">
        <div class="kpi-box">
            <h2 style="color: #FF9800;">Total Arrecadado (HOJE)</h2>
            <p style="color: #FF9800;">{kpis_dia['total_fmt']}</p>
        </div>
        <div class="kpi-box">
            <h2 style="color: #00BCD4;">Contagem de Vendas (HOJE)</h2>
            <p style="color: #00BCD4;">{kpis_dia['contagem']}</p>
        </div>
        <div class="kpi-box">
            <h2 style="color: #FF5722;">Pico de Vendas (HOJE)</h2>
            <p style="color: #FF5722;">{kpis_dia['pico_hora']}h</p>
        </div>
    </div>
    """

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Dashboard Multicamadas de Vendas</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0"> 
        {styles}
    </head>
    <body>
        <h1 style="text-align: center; color: #64ffda;">Painel de Controle de Vendas (Mês e Dia)</h1>
        <p style="text-align: center; color: #aaa;">Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
        
        <div class="header-section">
            <h2 style="color: #2196F3;">Contexto Mensal (Acumulado)</h2>
        </div>
        {kpi_mes_html}
        
        <div class="chart-container">
            <div class="chart-item">
                {fig_sabor.to_html(full_html=False, include_plotlyjs='cdn')}
            </div>
             <div class="chart-item">
                {fig_cliente.to_html(full_html=False, include_plotlyjs='cdn')}
            </div>
        </div>

        <div class="header-section">
            <h2 style="color: #FF9800;">Foco Diário (Até Agora)</h2>
        </div>
        {kpi_dia_html}

        <div class="chart-container">
            <div class="chart-item" style="width: 100%;">
                {fig_hora.to_html(full_html=False, include_plotlyjs='cdn')}
            </div>
        </div>
        
    </body>
    </html>
    """
    
    return html_content

# --- 4. EXECUÇÃO PRINCIPAL (SAÍDA CORRETA) ---
if __name__ == "__main__":
    try:
        df_completo, df_mes, df_dia = carregar_e_limpar_dados() 
        final_html = criar_dashboard_html(df_completo, df_mes, df_dia)

        # Usando o nome de arquivo original para sobrescrever o que o GitHub Pages exibe.
        with open("dashboard_vendas_final.html", "w") as f:
            f.write(final_html)

        print("Dashboard HTML multicamadas FINAL gerado com sucesso.")

    except ValueError as ve:
        error_html = f"""
        <html><body>
            <h1 style='color: red;'>ERRO CRÍTICO NA LIMPEZA DE DADOS 🛑</h1>
            <p><strong>André</strong>, o DataFrame está vazio após a limpeza. Verifique o formato dos dados (DD/MM/AAAA HH:MM:SS) ou a planilha.</p>
            <p>Detalhe: {ve}</p>
        </body></html>
        """
        with open("dashboard_erro.html", "w") as f:
            f.write(error_html)
        print(f"ERRO DE DADOS: {ve}. Um arquivo 'dashboard_erro.html' foi gerado para debug.")
        exit(1)

    except Exception as e:
        print(f"Ocorreu um erro INESPERADO no script de automação: {e}")
        exit(1)
