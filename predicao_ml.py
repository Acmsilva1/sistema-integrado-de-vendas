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

# --- CONFIGURAÇÕES DE DADOS ---
# ID ÚNICO da planilha que deve ser acessada (já verificado)
ID_PLANILHA_UNICA = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y"

ABA_VENDAS = "VENDAS"
ABA_GASTOS = "GASTOS" 

# Colunas
COLUNA_VALOR_VENDA = 'VALOR DA VENDA'
COLUNA_COMPRADOR = 'DADOS DO COMPRADOR' 
COLUNA_ITEM_VENDIDO = 'SABORES'       

COLUNA_VALOR_GASTO = 'VALOR' 
COLUNA_DATA = 'DATA E HORA' 

OUTPUT_HTML = "dashboard_ml_insights.html"
URL_DASHBOARD = "https://acmsilva1.github.io/analise-de-vendas/dashboard_ml_insights.html" 
# --------------------------------------------------------------------------------

def format_brl(value):
    """Função helper para formatar valores em R$"""
    value = float(value)
    return f"R$ {value:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ',')

def autenticar_gspread():
    SHEET_CREDENTIALS_JSON = os.environ.get('GCP_SA_CREDENTIALS')
    if not SHEET_CREDENTIALS_JSON:
        raise ConnectionError("Variável de ambiente 'GCP_SA_CREDENTIALS' não encontrada. O fluxo vai falhar!")
    credentials_dict = json.loads(SHEET_CREDENTIALS_JSON) 
    return gspread.service_account_from_dict(credentials_dict)

def carregar_dados_de_planilha(gc, sheet_id, aba_nome, coluna_valor, prefixo):
    """
    Carrega os dados da aba. Implementa a correção de governança para 
    capturar especificamente o erro de WorksheetNotFound ou 404.
    """
    print(f"DEBUG: Carregando dados: ID={sheet_id}, Aba={aba_nome}")
    try:
        planilha = gc.open_by_key(sheet_id).worksheet(aba_nome)
        dados = planilha.get_all_values()
        
        if not dados or len(dados) < 2:
             print(f"Alerta: Planilha {aba_nome} está vazia.")
             return pd.DataFrame()
             
        df = pd.DataFrame(dados[1:], columns=dados[0])
        
        # Limpeza e conversão de Valor
        df['temp_valor'] = df[coluna_valor].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=True).str.strip()
        df[f'{prefixo}_Float'] = pd.to_numeric(df['temp_valor'], errors='coerce')
        
        # Limpeza e conversão de Data
        df['Data_Datetime'] = pd.to_datetime(df[COLUNA_DATA], errors='coerce', dayfirst=True)
        
        df_validos = df.dropna(subset=['Data_Datetime', f'{prefixo}_Float']).copy()
        
        # Se for VENDAS, retorna o DF BRUTO
        if aba_nome == ABA_VENDAS:
             return df_validos
        
        # Agrupamento Mensal para GASTOS
        df_validos['Mes_Ano'] = df_validos['Data_Datetime'].dt.to_period('M')
        df_mensal = df_validos.groupby('Mes_Ano')[f'{prefixo}_Float'].sum().reset_index()
        df_mensal.columns = ['Mes_Ano', f'Total_{prefixo}']
        
        return df_mensal.set_index('Mes_Ano')
        
    except WorksheetNotFound:
        # CORREÇÃO CRÍTICA APLICADA: Tratamento específico do erro 404/aba
        print(f"ERRO CRÍTICO: Aba '{aba_nome}' não encontrada! Verifique se está em MAIÚSCULAS.")
        return pd.DataFrame() 
    except Exception as e:
        print(f"ERRO ao carregar {aba_nome}: {e}")
        return pd.DataFrame()

def carregar_e_combinar_dados(gc):
    df_vendas_bruto = carregar_dados_de_planilha(gc, ID_PLANILHA_UNICA, ABA_VENDAS, COLUNA_VALOR_VENDA, 'Vendas')
    df_gastos_mensal = carregar_dados_de_planilha(gc, ID_PLANILHA_UNICA, ABA_GASTOS, COLUNA_VALOR_GASTO, 'Gastos')
    
    if df_vendas_bruto.empty or df_gastos_mensal.empty:
        raise ValueError("Dados insuficientes para análise de Lucro (Vendas ou Gastos estão vazios).")

    # 1. Consolidação Mensal de Vendas
    df_vendas_mensal = df_vendas_bruto.copy()
    df_vendas_mensal['Mes_Ano'] = df_vendas_mensal['Data_Datetime'].dt.to_period('M')
    df_vendas_mensal = df_vendas_mensal.groupby('Mes_Ano')['Vendas_Float'].sum().reset_index().set_index('Mes_Ano')
    df_vendas_mensal.columns = ['Total_Vendas']
    
    # 2. Combinar
    df_combinado = pd.merge(
        df_vendas_mensal, 
        df_gastos_mensal, 
        left_index=True, 
        right_index=True, 
        how='outer' 
    ).fillna(0) 

    df_combinado['Lucro_Liquido'] = df_combinado['Total_Vendas'] - df_combinado['Total_Gastos']
    
    df_combinado = df_combinado.sort_index().reset_index()
    df_combinado['Mes_Index'] = np.arange(len(df_combinado))
    
    if len(df_combinado) < 4:
        raise ValueError(f"Dados insuficientes para ML: Apenas {len(df_combinado)} meses consolidados. Mínimo de 4 meses é recomendado.")
            
    return df_combinado, df_vendas_bruto

def treinar_e_prever(df_mensal):
    X = df_mensal[['Mes_Index']] 
    y = df_mensal['Lucro_Liquido'] 
    
    modelo = LinearRegression()
    modelo.fit(X, y)
    
    proximo_mes_index = df_mensal['Mes_Index'].max() + 1
    previsao_proximo_mes = modelo.predict([[proximo_mes_index]])[0]

    # Métrica de Governança de IA: MAE
    predicoes_historicas = modelo.predict(X)
    mae = mean_absolute_error(y, predicoes_historicas)

    ultimo_lucro_real = df_mensal['Lucro_Liquido'].iloc[-1]
    
    return previsao_proximo_mes, mae, ultimo_lucro_real

def analisar_metricas_negocio(df_vendas_bruto, ano_foco):
    """
    Calcula o Melhor Comprador e o Sabor Mais Vendido (baseado em receita),
    FILTRANDO apenas para o ano de foco.
    """
    df_filtrado = df_vendas_bruto[
        df_vendas_bruto['Data_Datetime'].dt.year == ano_foco
    ].copy()
    
    if df_filtrado.empty:
        return f"N/A ({ano_foco} sem dados)", f"N/A ({ano_foco} sem dados)"
        
    if COLUNA_COMPRADOR not in df_filtrado.columns or COLUNA_ITEM_VENDIDO not in df_filtrado.columns:
        return "N/A (Colunas Faltantes)", "N/A (Colunas Faltantes)"
        
    # Melhor Comprador
    comprador_df = df_filtrado.groupby(COLUNA_COMPRADOR)['Vendas_Float'].sum().reset_index()
    if comprador_df.empty:
        return "N/A (Dados vazios)", "N/A (Dados vazios)"
        
    melhor_comprador = comprador_df.sort_values(by='Vendas_Float', ascending=False).iloc[0]
    
    # Sabor/Produto Mais Vendido
    produto_df = df_filtrado.groupby(COLUNA_ITEM_VENDIDO)['Vendas_Float'].sum().reset_index()
    produto_mais_vendido = produto_df.sort_values(by='Vendas_Float', ascending=False).iloc[0]

    # Formata os resultados
    resultado_comprador = (
        f"{melhor_comprador[COLUNA_COMPRADOR]} ({format_brl(melhor_comprador['Vendas_Float'])})"
    )
    
    resultado_produto = (
        f"{produto_mais_vendido[COLUNA_ITEM_VENDIDO]} ({format_brl(produto_mais_vendido['Vendas_Float'])})"
    )

    return resultado_comprador, resultado_produto

def gerar_tabela_auditoria(df_mensal):
    """Gera o HTML da tabela histórica de Lucro, Vendas e Gastos (COMPLETA)."""
    table_rows = ""
    for index, row in df_mensal.iterrows():
        lucro = row['Lucro_Liquido']
        lucro_class = 'lucro-positivo-dark' if lucro >= 0 else 'lucro-negativo-dark'
        
        mes_formatado = row['Mes_Ano'].strftime('%Y-%m')
        
        table_rows += f"""
        <tr class="{lucro_class}">
            <td>{mes_formatado}</td>
            <td>{format_brl(row['Total_Vendas'])}</td>
            <td>{format_brl(row['Total_Gastos'])}</td>
            <td>{format_brl(lucro)}</td>
        </tr>
        """
    return table_rows

def gerar_html_balanco_grafico(df_dados, titulo_secao):
    """Gera o HTML da tabela de balanço mensal com barras visuais, REUTILIZÁVEL."""
    
    lucro_html = ""
    
    if df_dados.empty:
        return f"<p>Não há dados de Lucro Mensal para {titulo_secao}.</p>"
        
    df_dados['Lucro_Abs'] = df_dados['Lucro_Liquido'].abs()
    max_lucro = df_dados['Lucro_Abs'].max()

    for index, row in df_dados.iterrows():
        lucro = row['Lucro_Liquido']
        cor_barra = '#006400' if lucro >= 0 else '#9c0000' # Verde/Vermelho escuro
        largura = (row['Lucro_Abs'] / max_lucro) * 100 if max_lucro > 0 else 0 
        
        mes_formatado = row['Mes_Ano'].strftime('%b/%Y') 

        lucro_html += f"""
        <tr>
            <td>{mes_formatado}</td>
            <td>
                <div style="background-color: #2c2c2c; border-radius: 4px; overflow: hidden; height: 20px; text-align: left;">
                    <div style="width: {largura}%; background-color: {cor_barra}; height: 100%; text-align: right; line-height: 20px; color: white; padding-right: 5px; box-sizing: border-box;">
                        {format_brl(lucro)}
                    </div>
                </div>
            </td>
        </tr>
        """
        
    html_final = f"""
    <table>
        <thead>
            <tr>
                <th style="width: 20%;">Mês/Ano</th>
                <th>Lucro Líquido (Visualização)</th>
            </tr>
        </thead>
        <tbody>
            {lucro_html}
        </tbody>
    </table>
    """
    return html_final

def montar_dashboard_ml(previsao, mae, ultimo_valor_real, df_historico, melhor_comprador_atual, produto_mais_vendido_atual, melhor_comprador_ant, produto_mais_vendido_ant, ano_ant, ano_atual):
    
    # Lógica de Classificação do Insight
    diferenca = previsao - ultimo_valor_real
    
    if previsao < 0:
        insight = f"🚨 **Previsão de PREJUÍZO!** Lucro negativo de {format_brl(abs(previsao))} esperado. Hora de cortar o cafezinho."
        cor = "#9c0000" 
    elif diferenca > (ultimo_valor_real * 0.10):
        insight = f"🚀 **Crescimento de Lucro Esperado!** Aumento de {format_brl(diferenca)}. Suas vendas estão no *hype*!"
        cor = "#006400" 
    elif diferenca < -(ultimo_valor_real * 0.10):
        insight = f"⚠️ **Risco de Queda de Lucro!** Retração de {format_brl(abs(diferenca))} esperada. Analise seus custos ou chame o Batman!"
        cor = "#b8860b" 
    else:
        insight = f"➡️ **Estabilidade Esperada.** Lucro projetado próximo ao mês passado. Nem frio, nem quente."
        cor = "#005a8d" 
    
    texto_box_cor = "white"

    tabela_auditoria_html = gerar_tabela_auditoria(df_historico)
    
    # --- FILTRAGEM PARA GRÁFICOS DE BALANÇO ---
    df_balanco_anterior = df_historico[df_historico['Mes_Ano'].dt.year == ano_ant].copy()
    html_balanco_anterior = gerar_html_balanco_grafico(df_balanco_anterior, f"o Ano de {ano_ant}")

    df_balanco_atual = df_historico[df_historico['Mes_Ano'].dt.year == ano_atual].copy()
    html_balanco_atual = gerar_html_balanco_grafico(df_balanco_atual, f"o Ano de {ano_atual}")
    
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Dashboard ML Insights - Previsão de Lucro Líquido</title>
         <style>
            /* --- ESTILOS DARK MODE EXCLUSIVO --- */
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #121212; color: #e0e0e0; }}
            .container {{ max-width: 900px; margin: auto; background: #1e1e1e; padding: 20px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.5); }}
            h2 {{ color: #bb86fc; border-bottom: 2px solid #bb86fc; padding-bottom: 10px; }}
            h3 {{ color: #03dac6; margin-top: 25px; }}
            
            /* Metric Box (Cor baseada na previsão) */
            .metric-box {{ padding: 20px; margin-bottom: 20px; border-radius: 8px; background-color: {cor}; color: {texto_box_cor}; text-align: center; }}
            .metric-box h3 {{ margin-top: 0; font-size: 1.5em; }}
            .metric-box p {{ font-size: 2.5em; font-weight: bold; }}
            
            .info-box {{ padding: 10px; border: 1px dashed #444; background-color: #2c2c2c; margin-top: 15px; }}
            
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
            th, td {{ padding: 10px; border: 1px solid #333; text-align: left; }}
            th {{ background-color: #3700b3; color: white; }}
            
            /* Cores de Fundo da Tabela no Dark Mode */
            .lucro-positivo-dark {{ background-color: #1f311f; color: #c7ecc7; }} 
            .lucro-negativo-dark {{ background-color: #3b1f1f; color: #ffbaba; }} 
            
            .metric-card {{ background: #2c2c2c; padding: 15px; border-radius: 6px; box-shadow: 0 2px 4px rgba(0,0,0,0.2); margin-top: 10px; }}
            .metric-card h4 {{ color: #03dac6; margin-top: 0; }}
            .metric-card p {{ font-size: 1.1em; font-weight: bold; color: #e0e0e0; }}
            .grid-2 {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin-top: 20px; }}
            a {{ color: #bb86fc; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>🔮 Insights de Machine Learning e Negócios</h2>
            <p>Modelo: Regressão Linear Simples. Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}. Foco do ML: Previsão de {ano_atual}.</p>
            
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
                <p>A governança de IA exige que você monitore o MAE: quanto menor, melhor a previsão histórica. </p>
            </div>
            
            <hr style="margin-top: 30px; border-color: #3700b3;">

            <h2>🏺 Baú de Memórias - Performance de {ano_ant}</h2>
            <p>Os resultados de {ano_ant} que serviram de base para treinar seu modelo de ML. A história é escrita por quem vende mais.</p>
            
            <h3>Resumo de KPIs Chave ({ano_ant})</h3>
            <div class="grid-2">
                 <div class="metric-card">
                    <h4>Melhor Comprador Histórico ({ano_ant})</h4>
                    <p>{melhor_comprador_ant}</p>
                </div>
                 <div class="metric-card">
                    <h4>Sabor Mais Vendido Histórico ({ano_ant})</h4>
                    <p>{produto_mais_vendido_ant}</p>
                </div>
            </div>
            
            <h3>Balanço Mensal Detalhado de Lucro Líquido ({ano_ant})</h3>
            <p>O gráfico visual do desempenho mês a mês completo do ano passado.</p>
            {html_balanco_anterior}
            
            <hr style="margin-top: 30px; border-color: #3700b3;">
            <h2>🏆 Principais Indicadores de Negócio ({ano_atual})</h2>
            <p>Métricas de negócio baseadas nos dados brutos do ano corrente, essenciais para tomada de decisão AGORA.</p>
            <div class="grid-2">
                 <div class="metric-card">
                    <h4>Melhor Comprador (Receita Gerada)</h4>
                    <p>{melhor_comprador_atual}</p>
                </div>
                 <div class="metric-card">
                    <h4>Sabor Mais Vendido (Receita Gerada)</h4>
                    <p>{produto_mais_vendido_atual}</p>
                </div>
            </div>

            <h2>📈 Análise de Lucro Mensal (Foco em {ano_atual})</h2>
            <p>Visualização da performance de Lucro Líquido no ano corrente. O tamanho da barra indica a magnitude do valor.</p>
            {html_balanco_atual}
            
            <h2>📊 Tabela de Auditoria Histórica (Base do ML)</h2>
            <p>Estes são os dados consolidados de Vendas e Gastos utilizados para treinar o modelo de previsão. A história completa e sequencial.</p>
            <table>
                <thead>
                    <tr>
                        <th>Mês/Ano</th>
                        <th>Vendas Totais</th>
                        <th>Gastos Totais</th>
                        <th>Lucro Líquido (Vendas - Gastos)</th>
                    </tr>
                </thead>
                <tbody>
                    {tabela_auditoria_html}
                </tbody>
            </table>

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
        
        df_mensal, df_vendas_bruto = carregar_e_combinar_dados(gc) 
        
        if not df_mensal.empty:
            
            # Identificação dos Anos
            ano_atual = df_vendas_bruto['Data_Datetime'].dt.year.max()
            ano_ant = ano_atual - 1 

            previsao, mae, ultimo_lucro_real = treinar_e_prever(df_mensal)
            
            # KPI 1: Métricas de Negócio (Ano Corrente - 2026)
            melhor_comprador_atual, produto_mais_vendido_atual = analisar_metricas_negocio(df_vendas_bruto, ano_atual)
            
            # KPI 2: Métricas de Negócio (Ano Anterior - 2025) - O BAÚ DE MEMÓRIAS!
            melhor_comprador_ant, produto_mais_vendido_ant = analisar_metricas_negocio(df_vendas_bruto, ano_ant)

            montar_dashboard_ml(
                previsao, 
                mae, 
                ultimo_lucro_real, 
                df_mensal,
                melhor_comprador_atual,
                produto_mais_vendido_atual,
                melhor_comprador_ant,
                produto_mais_vendido_ant,
                ano_ant,
                ano_atual
            )
        else:
            print("Execução ML interrompida por falta de dados históricos.")
            
    except Exception as e:
        error_message = str(e)
        print(f"ERRO CRÍTICO NA EXECUÇÃO DO ML: {error_message}")
        # Geração de arquivo de erro para governança
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
             f.write(f"<html><body><h2>Erro Crítico na Geração do ML Dashboard</h2><p>Detalhes: {error_message}</p><p>Ação: Verifique o ID da Planilha, as permissões de acesso do Service Account ({os.environ.get('GCP_SA_CREDENTIALS')}), ou os nomes das abas/colunas: VENDAS e GASTOS.</p></body></html>")
