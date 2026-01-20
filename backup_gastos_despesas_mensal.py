import gspread
import os 
import json 
import sys
from datetime import datetime

# IDs das planilhas - Mantendo sua governança de variáveis
PLANILHA_ORIGEM_ID = "1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug"
PLANILHA_HISTORICO_ID = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y"

MAP_ABAS = {
    "vendas": "VENDAS",
    "gastos": "GASTOS"
}

def autenticar_gspread():
    credenciais_json_string = os.environ.get('GSPREAD_SERVICE_ACCOUNT_CREDENTIALS')
    if not credenciais_json_string:
        raise Exception("Variável de ambiente GSPREAD_SERVICE_ACCOUNT_CREDENTIALS não encontrada!")
    
    credenciais_dict = json.loads(credenciais_json_string)
    return gspread.service_account_from_dict(credenciais_dict)

def fazer_backup_inteligente(gc, p_origem_id, p_dest_id, aba_origem_name, aba_dest_name):
    print(f"\n🔍 Verificando: {aba_origem_name.upper()}...")
    
    try:
        # Abre as abas
        aba_origem = gc.open_by_key(p_origem_id).worksheet(aba_origem_name)
        aba_dest = gc.open_by_key(p_dest_id).worksheet(aba_dest_name)
        
        # Pega todos os valores (Cuidado com LGPD: não logar dados sensíveis aqui)
        dados_origem = aba_origem.get_all_values()
        dados_dest = aba_dest.get_all_values()

        if len(dados_origem) <= 1:
            print(f"ℹ️ Aba '{aba_origem_name}' está vazia ou apenas com cabeçalho.")
            return

        # Criamos um set de strings para comparação rápida (Otimização de performance)
        # Cada linha vira uma string única separada por vírgula
        set_historico = set([",".join(map(str, linha)) for linha in dados_dest])

        novos_dados = []
        for linha in dados_origem[1:]: # Ignora cabeçalho da origem
            hash_linha = ",".join(map(str, linha))
            if hash_linha not in set_historico:
                novos_dados.append(linha)

        if novos_dados:
            aba_dest.append_rows(novos_dados, value_input_option='USER_ENTERED')
            print(f"✅ {len(novos_dados)} novas linhas consolidadas em '{aba_dest_name}'.")
        else:
            print(f"😴 Nada novo em '{aba_origem_name}'. Tudo já está no backup.")

    except Exception as e:
        print(f"❌ Erro ao processar '{aba_origem_name}': {e}")

def main():
    # Removida a trava de 'dia 1' para permitir execução diária
    print(f"📅 Execução Diária: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    
    gc = autenticar_gspread()
    for origem, destino in MAP_ABAS.items():
        fazer_backup_inteligente(gc, PLANILHA_ORIGEM_ID, PLANILHA_HISTORICO_ID, origem, destino)
        
    print("\n🏁 Processo de sincronização concluído.")

if __name__ == "__main__":
    main()
