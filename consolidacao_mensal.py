import gspread
import os 
import json 

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

# Abre a planilha de origem (o espelho). Aba: "vendas" (minúsculo).
planilha_origem = gc.open_by_key("1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug").worksheet("vendas")
dados_do_mes = planilha_origem.get_all_values()

# Abre a planilha de destino (o Histórico Anual). Aba: "VENDAS" (MAIÚSCULO).
planilha_historico = gc.open_by_key("1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y").worksheet("VENDAS")

# 3. APÊNDICE: Insere os dados APÓS a última linha preenchida
# [1:] exclui o cabeçalho.
planilha_historico.append_rows(dados_do_mes[1:], value_input_option='USER_ENTERED')

print("Backup mensal concluído e consolidado no Histórico Anual.")

# AGORA É COM VOCÊ: Após a confirmação do backup, você pode apagar
# as linhas da planilha RAW do Forms manualmente.
