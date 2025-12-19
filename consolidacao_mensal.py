import gspread

# 1. Autenticação (Assumindo que você já tem o service account configurado)
gc = gspread.service_account(filename='caminho/do/seu/credenciais.json')

# 2. ABRIR AS PLANILHAS
# Abre a planilha de origem (o espelho com os dados do mês recém-encerrado)
planilha_origem = gc.open_by_key("1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug").worksheet(0)
dados_do_mes = planilha_origem.get_all_values()

# Abre a planilha de destino (o Histórico Anual no Google Drive)
planilha_historico = gc.open_by_key("1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y").worksheet(0)

# 3. APÊNDICE: Insere os dados APÓS a última linha preenchida
# [1:] é crucial, pois exclui o cabeçalho do seu array de dados,
# garantindo que o cabeçalho não seja repetido na planilha histórica.
planilha_historico.append_rows(dados_do_mes[1:], value_input_option='USER_ENTERED')

print("Backup mensal concluído e consolidado no Histórico Anual.")

# AGORA É COM VOCÊ: Após a confirmação do backup, você pode apagar
# as linhas da planilha RAW do Forms manualmente.