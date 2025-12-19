import gspread
import pandas as pd
from datetime import datetime
import os

# --- Configurações ---
ID_HISTORICO = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y"
OUTPUT_HTML = "dashboard_historico.html"

def gerar_analise_historica():
    try:
        # 1. Autenticação e Leitura
        gc = gspread.service_account(filename='credenciais.json')
        planilha_historico = gc.open_by_key(ID_HISTORICO).worksheet(0)
        
        # Pega todos os dados (incluindo cabeçalho)
        dados = planilha_historico.get_all_values()
        
        # O cabeçalho é a primeira linha
        headers = dados[0]
        data = dados[1:]

        # 2. Criação do DataFrame com Pandas
        df = pd.DataFrame(data, columns=headers)
        
        # --- Mapeamento das Colunas Chave ---
        COLUNA_DATA = 'DATA E HORA'
        COLUNA_VALOR = 'VALOR DA VENDA'
        
        # 3. Tratamento e Limpeza
        
        # Converte a coluna de valor para numérico, tratando erros
        # Substitui vírgulas por pontos, se necessário (padrão brasileiro)
        df[COLUNA_VALOR] = df[COLUNA_VALOR].str.replace(',', '.', regex=True)
        df['Valor_Venda_Float'] = pd.to_numeric(df[COLUNA_VALOR], errors='coerce')
        
        # Converte a coluna de Data para datetime
        df['Data_Datetime'] = pd.to_datetime(df[COLUNA_DATA], errors='coerce')
        
        # Remove linhas com valores nulos nas colunas chave
        df.dropna(subset=['Data_Datetime', 'Valor_Venda_Float'], inplace=True)
        
        # --- 4. ANÁLISE DE TENDÊNCIAS E VENDAS MENSAIS ---

        # Cria uma coluna de Mês/Ano para agrupar
        df['Mes_Ano'] = df['Data_Datetime'].dt.to_period('M')
        
        # a) Vendas Mensais: Agrupa e soma
        vendas_mensais = df.groupby('Mes_Ano')['Valor_Venda_Float'].sum().reset_index()
        vendas_mensais['Mes_Ano'] = vendas_mensais['Mes_Ano'].astype(str) 

        # b) Tendências (Variação Percentual Mês a Mês)
        vendas_mensais['Vendas_Anteriores'] = vendas_mensais['Valor_Venda_Float'].shift(1)
        vendas_mensais['Variacao_Mensal'] = (
            (vendas_mensais['Valor_Venda_Float'] - vendas_mensais['Vendas_Anteriores']) / vendas_mensais['Vendas_Anteriores']
        ) * 100
        
        # c) Insights Simples (Último Mês)
        if len(vendas_mensais) > 0:
            ultimo_mes = vendas_mensais.iloc[-1]
            tendencia = ultimo_mes['Variacao_Mensal']
            
            if pd.isna(tendencia):
                insight_tendencia = "Início da análise. Ainda não há tendência Mês-a-Mês."
            elif tendencia > 5:
                insight_tendencia = f"🚀 Forte crescimento de {tendencia:.2f}% no último mês! Mantenha a estratégia."
            elif tendencia > 0:
                insight_tendencia = f"📈 Crescimento moderado de {tendencia:.2f}%. O mercado está em expansão controlada."
            else:
                insight_tendencia = f"📉 Queda de {tendencia:.2f}%. Reveja o plano de vendas imediatamente."
        else:
            insight_tendencia = "Ainda não há dados suficientes no Histórico para gerar a análise."


        # --- 5. GERAÇÃO DO DASHBOARD HTML (Mesmo Layout anterior) ---

        html_content = f"""
        <html>
        <head>
            <title>Dashboard Histórico de Vendas - Tendências</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f4f7f6; color: #333; }}
                .container {{ max-width: 900px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
                h2 {{ color: #007bff; border-bottom: 2px solid #007bff; padding-bottom: 10px; }}
                .metric-box {{ padding: 15px; margin-bottom: 15px; border-radius: 6px; }}
                .insight {{ background-color: #e9ecef; border-left: 5px solid #007bff; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
                th, td {{ padding: 12px; border: 1px solid #ddd; text-align: left; }}
                th {{ background-color: #007bff; color: white; }}
                .positivo {{ color: green; font-weight: bold; }}
                .negativo {{ color: red; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>📊 Análise Histórica e Tendências de Vendas (Total: R$ {vendas_mensais['Valor_Venda_Float'].sum():,.2f})</h2>
                <p>Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} (Lendo {len(df)} registros válidos)</p>
                
                <div class="metric-box insight">
                    <h3>Insights de Tendência</h3>
                    <p>{insight_tendencia}</p>
                </div>

                <h2>📈 Vendas Consolidadas Mês a Mês</h2>
                <table>
                    <tr>
                        <th>Mês/Ano</th>
                        <th>Total de Vendas</th>
                        <th>Tendência Mensal</th>
                    </tr>
                    {vendas_mensais.to_html(
                        classes='table', 
                        index=False, 
                        formatters={
                            'Valor_Venda_Float': 'R$ {:,.2f}'.format,
                            'Variacao_Mensal': lambda x: f'<span class="{"positivo" if x > 0 else "negativo"}">{x:.2f}%</span>' if pd.notna(x) else 'N/A'
                        },
                        columns=['Mes_Ano', 'Valor_Venda_Float', 'Variacao_Mensal']
                    ).replace('<thead>', '').replace('</thead>', '').replace('<tbody>', '').replace('</tbody>', '').replace('<tr>', '<tr>').replace('<td>', '<td>')}
                </table>
                
            </div>
        </body>
        </html>
        """

        # 6. Salva o HTML
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Análise Histórica concluída! {OUTPUT_HTML} gerado com sucesso.")

    except Exception as e:
        print(f"Erro na análise histórica: {e}")
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
             f.write(f"<html><body><h2>Erro na Geração do Dashboard Histórico</h2><p>Detalhes: {e}</p></body></html>")
        
if __name__ == "__main__":
    gerar_analise_historica()