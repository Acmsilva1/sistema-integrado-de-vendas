import gspread
import os 
import json 

# --- CONFIGURAÇÕES DAS PLANILHAS DE DESPESAS (CORRIGIDO) ---
# ID da planilha de origem (espelho do Forms)
PLANILHA_ORIGEM_ID = "1kpyo2IpxIdllvc43WR4ijNPCKTsWHJlQDk8w9EjhwP8"
# Nome da aba da planilha de origem (CORRIGIDO: 'gastos' em minúsculo)
ABA_ORIGEM_NAME = "gastos" 

# ID da planilha de destino (Histórico Anual de Despesas)
PLANILHA_HISTORICO_ID = "1DU3oxwCLCVmmYA9oD9lrGkBx2SyI87UtPw-BDDwA9EA"
# Nome da aba da planilha de destino (CORRIGIDO: 'GASTOS' em maiúsculo)
ABA_HISTORICO_NAME = "GASTOS"
# -----------------------------------------------------------


# 1. Autenticação (Usando o Secret do GitHub)
# -----------------------------------------------------------
credenciais_json_string = os.environ.get('GSPREAD_SERVICE_ACCOUNT_CREDENTIALS')

if not credenciais_json_string:
    raise Exception("Variável de ambiente GSPREAD_SERVICE_ACCOUNT_CREDENTIALS não encontrada! Verifique o Secret no GitHub (nome GCP_SA_CREDENTIALS).")

credenciais_dict = json.loads(credenciais_json_string)
gc = gspread.service_account_from_dict(credenciais_dict)
# -----------------------------------------------------------


# 2. ABRIR AS PLANILHAS USANDO O NOME DA ABA CORRETO (Case-Sensitive)
# -----------------------------------------------------------
try:
    # Abre a planilha de origem (o espelho) usando "gastos"
    print(f"Tentando abrir planilha de origem ID: {PLANILHA_ORIGEM_ID}, Aba: {ABA_ORIGEM_NAME}")
    planilha_origem = gc.open_by_key(PLANILHA_ORIGEM_ID).worksheet(ABA_ORIGEM_NAME)
    dados_do_mes = planilha_origem.get_all_values()
except Exception as e:
    print(f"Erro ao abrir planilha de origem de despesas: {e}")
    raise

try:
    # Abre a planilha de destino (o Histórico Anual) usando "GASTOS"
    print(f"Tentando abrir planilha de histórico ID: {PLANILHA_HISTORICO_ID}, Aba: {ABA_HISTORICO_NAME}")
    planilha_historico = gc.open_by_key(PLANILHA_HISTORICO_ID).worksheet(ABA_HISTORICO_NAME)
except Exception as e:
    print(f"Erro ao abrir planilha de histórico de despesas: {e}")
    raise
# -----------------------------------------------------------

# 3. APÊNDICE: Insere os dados APÓS a última linha preenchida
# [1:] exclui o cabeçalho.
if len(dados_do_mes) > 1:
    planilha_historico.append_rows(dados_do_mes[1:], value_input_option='USER_ENTERED')
    print("Backup mensal de despesas concluído e consolidado no Histórico Anual.")
else:
    print("Não há novas despesas para consolidar este mês (apenas cabeçalho).")
