import gspread
import pandas as pd
from datetime import datetime
import os
import json # Necessário para carregar o secret JSON

# --- Configurações ---
ID_HISTORICO = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y"
OUTPUT_HTML = "dashboard_historico.html"
URL_DASHBOARD = "https://acmsilva1.github.io/analise-de-vendas/dashboard_historico.html"
COLUNA_DATA = 'DATA E HORA'
COLUNA_VALOR = 'VALOR DA VENDA'

def autenticar_gspread():
    """
    Função de autenticação robusta, buscando credenciais do secret do GitHub Actions
    ou do arquivo local, garantindo código limpo e portabilidade.
    """
    try:
        # Tenta carregar o JSON de credenciais da variável de ambiente (CI/CD)
        SHEET_CREDENTIALS_JSON = os.environ.get('GCP_SA_CREDENTIALS')
        
        if SHEET_CREDENTIALS_JSON:
            credentials_dict = json.loads(SHEET_CREDENTIALS_JSON) 
            gc = gspread.service_account_from_dict(credentials_dict)
            print("Autenticação via Váriavel de Ambiente (Secret) concluída.")
            return gc
        else:
            # Caso não esteja no CI/CD, tenta o método local (credenciais.json)
            # Isto é útil apenas para testes locais.
            gc = gspread.service_account(filename='credenciais.json')
            print("Autenticação via arquivo local (credenciais.json) concluída.")
            return gc

    except Exception as e:
        # Lançamos uma exceção clara para evitar o erro vazio
        raise ConnectionError(f"Falha na autenticação do Google Sheets. Detalhes: {e}")


def gerar_analise_historica():
    total_vendas_global = 0 # Inicializado para ser usado no cabeçalho
    
    try:
        # 1. Autenticação (Agora robusta!)
        gc = autenticar_gspread()
        planilha_historico = gc.open_by_key(ID_HISTORICO).worksheet(0)
        
        dados = planilha_historico.get_all_values()
        headers = dados[0]
        data = dados[1:]

        df = pd.DataFrame(data, columns=headers)
        
        # 2. Tratamento e Limpeza (Governança de Dados)
        
        df[COLUNA_VALOR] = df[COLUNA_VALOR].astype(str).str.replace(',', '.', regex=True)
        df['Valor_Venda_Float'] = pd.to_numeric(df[COLUNA_VALOR], errors='coerce')
        
        # CORREÇÃO DE GOVERNANÇA DE DADOS: dayfirst=True para formato DD/MM/YYYY (BR)
        df['Data_Datetime'] = pd.to_datetime(df[COLUNA_DATA], errors='coerce', dayfirst=True)
        
        df.dropna(subset=['Data_Datetime', 'Valor_Venda_Float'], inplace=True)
        
        # --- 3. ANÁLISE DE TENDÊNCIAS E VENDAS MENSAIS ---

        df['Mes_Ano'] = df['Data_Datetime'].dt.to_period('M')
        vendas_mensais = df.groupby('Mes_Ano')['Valor_Venda_Float'].sum().reset_index()
        vendas_mensais['Mes_Ano'] = vendas_mensais['Mes_Ano'].astype(str) 

        vendas_mensais['Vendas_Anteriores'] = vendas_mensais['Valor_Venda_Float'].shift(1)
        vendas_mensais['Variacao_Mensal'] = (
            (vendas_mensais['Valor_Venda_Float'] - vendas_mensais['Vendas_Anteriores']) / vendas_mensais['Vendas_Anteriores']
        ) * 100
        
        total_vendas_global = vendas_mensais['Valor_Venda_Float'].sum()
        
        if not vendas_mensais.empty:
            ultimo_mes = vendas_mensais.iloc[-1]
            tendencia = ultimo_mes['Variacao_Mensal']
            
            # Insights mantidos
            if pd.isna(tendencia):
                insight_tendencia = "Início da análise. Ainda não há tendência Mês-a-Mês."
            elif tendencia > 5:
                insight_tendencia = f"🚀 Forte crescimento de {tendencia:.2f}% no último mês! Mantenha a estratégia."
            elif tendencia > 0:
                insight_tendencia = f"📈 Crescimento moderado de {tendencia:.2f}%. O mercado está em expansão controlada."
            else:
                insight_tendencia = f"📉 Queda de {tendencia:.2f}%. Reveja o plano de vendas imediatamente."
        else:
            insight_tendencia = "Nenhum dado válido encontrado após a limpeza. A planilha está vazia ou corrompida."


        # --- 4. GERAÇÃO DO DASHBOARD HTML (Código Limpo) ---

        # Função de formatação para injetar classes CSS no Pandas to_html
        def format_variacao(val):
            if pd.isna(val):
                return 'N/A'
            # Adiciona a formatação e a classe CSS condicionalmente
            classe = "positivo" if val > 0 else "negativo"
            # O truque é retornar a tag <td> formatada com o span
            return f'<td class="val-col"><span class="{classe}">{val:.2f}%</span></td>'

        # Geração da tabela em HTML usando Pandas to_html com formatação segura
        tabela_dados = vendas_mensais[['Mes_Ano', 'Valor_Venda_Float', 'Variacao_Mensal']].copy()

        # O Pandas é chato com formatters embutidos no to_html, vamos pré-formatar
        tabela_dados['Variacao_Mensal'] = tabela_dados['Variacao_Mensal'].apply(
            lambda x: f'<td class="val-col"><span class="{"positivo" if x > 0 else "negativo"}">{x:.2f}%</span></td>' if pd.notna(x) else '<td class="val-col">N/A</td>'
        )
        tabela_dados['Valor_Venda_Float'] = tabela_dados['Valor_Venda_Float'].apply('R$ {:,.2f}'.format)
        
        table_html = tabela_dados.to_html(
            classes='table', 
            index=False, 
            header=False
        )
        
        # O Pandas Styler não gera a tag <td> com a formatação. Vamos usar o to_html(header=False) e montar a tabela manualmente
        
        # Geração da Tabela usando string simples (mais seguro que to_html + replace)
        table_rows = ""
        for index, row in vendas_mensais[['Mes_Ano', 'Valor_Venda_Float', 'Variacao_Mensal']].iterrows():
             # Formata a variação com classe CSS
            variacao_display = f'<td class="val-col"><span class="{"positivo" if row["Variacao_Mensal"] > 0 else "negativo"}">{row["Variacao_Mensal"]:.2f}%</span></td>' if pd.notna(row["Variacao_Mensal"]) else '<td class="val-col">N/A</td>'
            
            # Formata o valor de venda (mantendo a formatação BRL)
            venda_display = f'<td class="val-col">R$ {row["Valor_Venda_Float"]:,.2f}</td>'
            
            table_rows += f"<tr><td>{row['Mes_Ano']}</td>{venda_display}{variacao_display}</tr>\n"


        html_content = f"""
        <!DOCTYPE html>
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
                <h2>📊 Análise Histórica e Tendências de Vendas (Total Global: R$ {total_vendas_global:,.2f})</h2>
                <p>Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} (Lendo {len(df)} registros válidos)</p>
                
                <div class="metric-box insight">
                    <h3>Insights de Tendência</h3>
                    <p>{insight_tendencia}</p>
                </div>

                <h2>📈 Vendas Consolidadas Mês a Mês</h2>
                <table class="table">
                    <thead>
                        <tr>
                            <th>Mês/Ano</th>
                            <th>Total de Vendas</th>
                            <th>Tendência Mensal</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
                <p style="margin-top: 20px; font-size: 0.9em; color: #777;">Dashboard hospedado em: <a href="{URL_DASHBOARD}" target="_blank">{URL_DASHBOARD}</a></p>
                
            </div>
        </body>
        </html>
        """

        # 5. Salva o HTML
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Análise Histórica concluída! {OUTPUT_HTML} gerado com sucesso.")

    except Exception as e:
        # Agora o erro deve ter detalhes mais úteis.
        print(f"Erro Crítico na Geração do Dashboard Histórico: {e}")
        # Gerando HTML de erro que mostra a exceção completa
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
             f.write(f"<html><body><h2>Erro Crítico na Geração do Dashboard Histórico</h2><p>Detalhes: {e}</p></body></html>")
        
if __name__ == "__main__":
    gerar_analise_historica()
