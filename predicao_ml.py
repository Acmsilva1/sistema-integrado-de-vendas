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

# --- CONFIGURAÇÕES DE DADOS (UNIFICADO E CORRIGIDO) ---
# Assumindo que a planilha de VENDAS tem o ID principal, já que ambas as abas estão nela.
ID_PLANILHA_UNICA = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y"

ABA_VENDAS = "VENDAS"
ABA_GASTOS = "GASTOS" # Colunas: DATA E HORA, PRODUTO, QUANTIDADE, VALOR

# Colunas (CORRIGIDAS E NOVAS)
COLUNA_VALOR_VENDA = 'VALOR DA VENDA'
COLUNA_COMPRADOR = 'DADOS DO COMPRADOR' # Para a métrica de Melhor Comprador (Aba VENDAS)
COLUNA_ITEM_VENDIDO = 'SABORES'       # Para a métrica de Sabor/Produto Mais Vendido (Aba VENDAS)

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
        raise ConnectionError("Variável de ambiente 'GCP_SA_CREDENTIALS' não encontrada.")
    credentials_dict = json.loads(SHEET_CREDENTIALS_JSON) 
    return gspread.service_account_from_dict(credentials_dict)

def carregar_dados_de_planilha(gc, sheet_id, aba_nome, coluna_valor, prefixo):
    """
    Carrega os dados da aba. Retorna o DF Bruto para VENDAS (para métricas de negócio) 
    ou o DF Agrupado Mensal para GASTOS (para lucro).
    """
    print(f"DEBUG: Carregando dados: ID={sheet_id}, Aba={aba_nome}")
    try:
        planilha = gc.open_by_key(sheet_id).worksheet(aba_nome)
        dados = planilha.get_all_values()
        
        if not dados or len(dados) < 2:
             print(f"Alerta: Planilha {aba_nome} está vazia.")
             return pd.DataFrame()
             
        df = pd.DataFrame(dados[1:], columns=dados[0])
        
        # Limpeza do Valor
        df['temp_valor'] = df[coluna_valor].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=True).str.strip()
        df[f'{prefixo}_Float'] = pd.to_numeric(df['temp_valor'], errors='coerce')
        
        # Limpeza da Data
        df['Data_Datetime'] = pd.to_datetime(df[COLUNA_DATA], errors='coerce', dayfirst=True)
        
        df_validos = df.dropna(subset=['Data_Datetime', f'{prefixo}_Float']).copy()
        
        # SE FOR VENDAS, RETORNA O DF BRUTO para permitir as análises de Comprador/Sabor (KPIs de Negócio)
        if aba_nome == ABA_VENDAS:
             return df_validos
        
        # Agrupamento Mensal para GASTOS
        df_validos['Mes_Ano'] = df_validos['Data_Datetime'].dt.to_period('M')
        df_mensal = df_validos.groupby('Mes_Ano')[f'{prefixo}_Float'].sum().reset_index()
        df_mensal.columns = ['Mes_Ano', f'Total_{prefixo}']
        
        return df_mensal.set_index('Mes_Ano')
        
    except Exception as e:
        print(f"ERRO ao carregar {aba_nome}: {e}")
        return pd.DataFrame()

def carregar_e_combinar_dados(gc):
    # Usamos ID_PLANILHA_UNICA para ambas as chamadas
    df_vendas_bruto = carregar_dados_de_planilha(gc, ID_PLANILHA_UNICA, ABA_VENDAS, COLUNA_VALOR_VENDA, 'Vendas')
    df_gastos_mensal = carregar_dados_de_planilha(gc, ID_PLANILHA_UNICA, ABA_GASTOS, COLUNA_VALOR_GASTO, 'Gastos')
    
    if df_vendas_bruto.empty or df_gastos_mensal.empty:
        raise ValueError("Dados insuficientes para análise de Lucro (Vendas ou Gastos estão vazios).")

    # 1. Preparar DF Vendas (BRUTO) para consolidação Mensal
    df_vendas_mensal = df_vendas_bruto.copy()
    df_vendas_mensal['Mes_Ano'] = df_vendas_mensal['Data_Datetime'].dt.to_period('M')
    df_vendas_mensal = df_vendas_mensal.groupby('Mes_Ano')['Vendas_Float'].sum().reset_index()
    df_vendas_mensal.columns = ['Mes_Ano', 'Total_Vendas']
    df_vendas_mensal = df_vendas_mensal.set_index('Mes_Ano')
    
    # 2. Combinar Mensalmente Vendas e Gastos
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
            
    # Retorna o consolidado mensal (para ML e auditoria) e o BRUTO de vendas (para métricas de negócio)
    return df_combinado, df_vendas_bruto

def treinar_e_prever(df_mensal):
    X = df_mensal[['Mes_Index']] 
    y = df_mensal['Lucro_Liquido'] 
    
    modelo = LinearRegression()
    modelo.fit(X, y)
    
    proximo_mes_index = df_mensal['Mes_Index'].max() + 1
    previsao_proximo_mes = modelo.predict([[proximo_mes_index]])[0]

    predicoes_historicas = modelo.predict(X)
    mae = mean_absolute_error(y, predicoes_historicas)

    ultimo_lucro_real = df_mensal['Lucro_Liquido'].iloc[-1]
    
    return previsao_proximo_mes, mae, ultimo_lucro_real

# --- NOVO: Função para extrair métricas de Negócio (Agora incluída e com nomes corrigidos) ---
def analisar_metricas_negocio(df_vendas_bruto):
    """Calcula o Melhor Comprador e o Sabor Mais Vendido (baseado em receita)."""
    # Verifica a existência das colunas CORRIGIDAS
    if COLUNA_COMPRADOR not in df_vendas_bruto.columns or COLUNA_ITEM_VENDIDO not in df_vendas_bruto.columns:
        print(f"Alerta: Colunas '{COLUNA_COMPRADOR}' ou '{COLUNA_ITEM_VENDIDO}' não encontradas no DF de Vendas.")
        return "N/A (Colunas de Negócio Faltantes)", "N/A (Colunas de Negócio Faltantes)"
        
    # Melhor Comprador
    comprador_df = df_vendas_bruto.groupby(COLUNA_COMPRADOR)['Vendas_Float'].sum().reset_index()
    if comprador_df.empty:
        return "N/A (Dados vazios)", "N/A (Dados vazios)"
        
    melhor_comprador = comprador_df.sort_values(by='Vendas_Float', ascending=False).iloc[0]
    
    # Sabor/Produto Mais Vendido
    produto_df = df_vendas_bruto.groupby(COLUNA_ITEM_VENDIDO)['Vendas_Float'].sum().reset_index()
    produto_mais_vendido = produto_df.sort_values(by='Vendas_Float', ascending=False).iloc[0]

    # Formata os resultados
    resultado_comprador = (
        f"{melhor_comprador[COLUNA_COMPRADOR]} ({format_brl(melhor_comprador['Vendas_Float'])})"
    )
    
    resultado_produto = (
        f"{produto_mais_vendido[COLUNA_ITEM_VENDIDO]} ({format_brl(produto_mais_vendido['Vendas_Float'])})"
    )

    return resultado_comprador, resultado_produto
# --------------------------------------------------------------------------------

def gerar_tabela_auditoria(df_mensal):
    """Gera o HTML da tabela histórica de Lucro, Vendas e Gastos."""
    table_rows = ""
    for index, row in df_mensal.iterrows():
        lucro = row['Lucro_Liquido']
        lucro_class = 'lucro-positivo' if lucro >= 0 else 'lucro-negativo'
        
        table_rows += f"""
        <tr class="{lucro_class}">
            <td>{row['Mes_Ano']}</td>
            <td>{format_brl(row['Total_Vendas'])}</td>
            <td>{format_brl(row['Total_Gastos'])}</td>
            <td>{format_brl(lucro)}</td>
        </tr>
        """
    return table_rows

def montar_dashboard_ml(previsao, mae, ultimo_valor_real, df_historico, melhor_comprador, produto_mais_vendido):
    
    # Lógica de Classificação do Insight
    diferenca = previsao - ultimo_valor_real
    
    if previsao < 0:
        insight = f"🚨 **Previsão de PREJUÍZO!** Lucro negativo de {format_brl(abs(previsao))} esperado. Hora de cortar o cafezinho."
        cor = "#dc3545" 
    elif diferenca > (ultimo_valor_real * 0.10):
        insight = f"🚀 **Crescimento de Lucro Esperado!** Aumento de {format_brl(diferenca)}. Suas vendas estão no *hype*!"
        cor = "#28a745" 
    elif diferenca < -(ultimo_valor_real * 0.10):
        insight = f"⚠️ **Risco de Queda de Lucro!** Retração de {format_brl(abs(diferenca))} esperada. Analise seus custos ou chame o Batman!"
        cor = "#ffc107" 
    else:
        insight = f"➡️ **Estabilidade Esperada.** Lucro projetado próximo ao mês passado. Nem frio, nem quente."
        cor = "#17a2b8" 

    tabela_auditoria_html = gerar_tabela_auditoria(df_historico)
    
    # Geração da Tabela de Análise Mensal de Lucro (Visualização Gráfica)
    lucro_anual_html = ""
    ultimo_ano = df_historico['Mes_Ano'].dt.year.max()
    df_ano_atual = df_historico[df_historico['Mes_Ano'].dt.year == ultimo_ano].copy()
    
    df_ano_atual['Lucro_Abs'] = df_ano_atual['Lucro_Liquido'].abs()
    max_lucro = df_ano_atual['Lucro_Abs'].max()

    for index, row in df_ano_atual.iterrows():
        lucro = row['Lucro_Liquido']
        cor_barra = '#28a745' if lucro >= 0 else '#dc3545'
        largura = (row['Lucro_Abs'] / max_lucro) * 100 if max_lucro > 0 else 0 

        lucro_anual_html += f"""
        <tr>
            <td>{row['Mes_Ano'].strftime('%b/%Y')}</td>
            <td>
                <div style="background-color: #eee; border-radius: 4px; overflow: hidden; height: 20px; text-align: left;">
                    <div style="width: {largura}%; background-color: {cor_barra}; height: 100%; text-align: right; line-height: 20px; color: white; padding-right: 5px; box-sizing: border-box;">
                        {format_brl(lucro)}
                    </div>
                </div>
            </td>
        </tr>
        """
    # 
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
            
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
            th, td {{ padding: 10px; border: 1px solid #ddd; text-align: left; }}
            th {{ background-color: #6f42c1; color: white; }}
            .lucro-positivo {{ background-color: #e6ffe6; }} 
            .lucro-negativo {{ background-color: #ffe6e6; }} 
            .text-negativo {{ color: red; font-weight: bold; }}
            .metric-card {{ background: #f8f9fa; padding: 15px; border-radius: 6px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); margin-top: 10px; }}
            .metric-card h4 {{ color: #007bff; margin-top: 0; }}
            .metric-card p {{ font-size: 1.1em; font-weight: bold; color: #333; }}
            .grid-2 {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin-top: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>🔮 Insights de Machine Learning e Negócios</h2>
            <p>Modelo: Regressão Linear Simples. Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
            
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
            
            <h2>🏆 Principais Indicadores de Negócio</h2>
            <p>Métricas de negócio baseadas nos dados brutos da aba VENDAS.</p>
            <div class="grid-2">
                 <div class="metric-card">
                    <h4>Melhor Comprador (Receita Gerada)</h4>
                    <p>{melhor_comprador}</p>
                </div>
                 <div class="metric-card">
                    <h4>Sabor Mais Vendido (Receita Gerada)</h4>
                    <p>{produto_mais_vendido}</p>
                </div>
            </div>

            <h2>📈 Análise de Lucro Mensal (Último Ano)</h2>
            <p>Visualização da performance de Lucro Líquido ao longo dos meses. O tamanho da barra indica a magnitude do valor.</p>
            <table>
                <thead>
                    <tr>
                        <th style="width: 20%;">Mês/Ano</th>
                        <th>Lucro Líquido (Visualização)</th>
                    </tr>
                </thead>
                <tbody>
                    {lucro_anual_html}
                </tbody>
            </table>
            
            <h2>📊 Tabela de Auditoria Histórica (Base do ML)</h2>
            <p>Estes são os dados consolidados de Vendas e Gastos utilizados para treinar o modelo de previsão.</p>
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


# --- EXECUÇÃO PRINCIPAL (CORRIGIDA) ---
if __name__ == "__main__":
    try:
        gc = autenticar_gspread()
        
        # O retorno AGORA É DUPLO: df_mensal (consolidado) e df_vendas_bruto (para métricas)
        df_mensal, df_vendas_bruto = carregar_e_combinar_dados(gc) 
        
        if not df_mensal.empty:
            previsao, mae, ultimo_lucro_real = treinar_e_prever(df_mensal)
            
            # NOVO: Calcular Métricas de Negócio
            melhor_comprador, produto_mais_vendido = analisar_metricas_negocio(df_vendas_bruto)
            
            # Passando todos os argumentos, incluindo as novas métricas
            montar_dashboard_ml(
                previsao, 
                mae, 
                ultimo_lucro_real, 
                df_mensal,
                melhor_comprador,
                produto_mais_vendido
            )
        else:
            print("Execução ML interrompida por falta de dados históricos.")
            
    except Exception as e:
        error_message = str(e)
        print(f"ERRO CRÍTICO NA EXECUÇÃO DO ML: {error_message}")
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
             f.write(f"<html><body><h2>Erro Crítico na Geração do ML Dashboard</h2><p>Detalhes: {error_message}</p><p>Verifique as credenciais, permissões de acesso ou os nomes das colunas: VENDAS (DATA E HORA, SABORES, DADOS DO COMPRADOR, VALOR DA VENDA), GASTOS (DATA E HORA, VALOR)</p></body></html>")
