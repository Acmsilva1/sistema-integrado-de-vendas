import gspread
import os 
import json 
import sys
from datetime import datetime

# --- CONFIGURAÇÕES DAS PLANILHAS ---

# IDs das planilhas (APENAS o ID)
PLANILHA_ORIGEM_ID = "1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug"  # Vendas e Gastos (Origem do mês)
PLANILHA_HISTORICO_ID = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y" # HISTORICO DE VENDAS E GASTOS (Destino)

# Mapeamento das Abas: {ABA_ORIGEM (minúscula): ABA_DESTINO (MAIÚSCULA)}
MAP_ABAS = {
    "vendas": "VENDAS",
    "gastos": "GASTOS"
}
# -----------------------------------------------------------


def autenticar_gspread():
    """Autentica o gspread usando a variável de ambiente."""
    credenciais_json_string = os.environ.get('GSPREAD_SERVICE_ACCOUNT_CREDENTIALS')

    if not credenciais_json_string:
        raise Exception("Variável de ambiente GSPREAD_SERVICE_ACCOUNT_CREDENTIALS não encontrada!")

    try:
        credenciais_dict = json.loads(credenciais_json_string)
        return gspread.service_account_from_dict(credenciais_dict)
    except Exception as e:
        raise Exception(f"Erro ao carregar ou autenticar credenciais JSON: {e}")


def fazer_backup(gc, planilha_origem_id, planilha_historico_id, aba_origem_name, aba_historico_name):
    """
    Função modularizada que copia os dados. A LIMPEZA DA ORIGEM AGORA É MANUAL.
    """
    print(f"\n--- Iniciando Backup: {aba_origem_name.upper()} para {aba_historico_name} ---")
    
    try:
        # 1. Abre a aba de origem e pega todos os dados
        planilha_origem = gc.open_by_key(planilha_origem_id).worksheet(aba_origem_name)
        dados_do_mes = planilha_origem.get_all_values()
        
        # 2. Verifica se há dados novos (dados_do_mes[1:] exclui o cabeçalho)
        dados_para_copiar = dados_do_mes[1:] 

        if not dados_para_copiar:
            print(f"Não há novos dados na aba '{aba_origem_name}' para consolidar (apenas cabeçalho).")
            return

        # 3. Abre a aba de destino (Histórico)
        planilha_historico = gc.open_by_key(planilha_historico_id).worksheet(aba_historico_name)
        
        # 4. Apêndice: Insere os dados no Histórico.
        planilha_historico.append_rows(dados_para_copiar, value_input_option='USER_ENTERED')
        
        print(f"Backup de {len(dados_para_copiar)} linhas concluído e consolidado na aba '{aba_historico_name}'.")
        print(f"=========================================================================")
        print(f"!!! ATENÇÃO !!!: A limpeza da aba de origem ('{aba_origem_name}') NÃO FOI FEITA.")
        print(f"PARA EVITAR DUPLICAÇÃO NO PRÓXIMO MÊS, LIMPE MANUALMENTE esta aba APÓS a confirmação.")
        print(f"=========================================================================")

        # O código de limpeza (batch_clear) foi REMOVIDO daqui.

    except gspread.exceptions.WorksheetNotFound as e:
        print(f"ERRO: A aba '{aba_origem_name}' ou '{aba_historico_name}' não foi encontrada.")
        raise RuntimeError(f"Falha na validação da Planilha: {e}") 
    except Exception as e:
        print(f"ERRO GRAVE durante o backup de {aba_origem_name}: {e}")
        raise


def main():
    """Função principal para orquestrar a execução e controlar a governança de tempo."""
    
    # Verifica se a execução foi forçada manualmente
    FORCA_EXECUCAO = os.environ.get('FORCA_EXECUCAO_MANUAL', 'false').lower() == 'true'
    hoje = datetime.now().day
    
    # -------------------------------------------------------------
    # Controle de Execução: Apenas no dia 1 (OU se for forçado)
    # -------------------------------------------------------------
    
    if hoje != 1 and not FORCA_EXECUCAO:
        print(f"Hoje é dia {hoje}. O Agente de Backup está dormindo (aguardando o dia 1 do mês).")
        sys.exit(0) 

    # Mensagem de Log
    if FORCA_EXECUCAO:
         print("\n🚨 AGENTE DE BACKUP ATIVADO (MANUAL OVERRIDE) - Executando sob demanda...")
    else:
         print(f"\n🚀 AGENTE DE BACKUP ATIVADO - Executando no dia {hoje}...")
    
    # 1. Autentica UMA VEZ
    gc = autenticar_gspread()
    
    # 2. Executa a função de backup para Vendas e Gastos (duas passagens)
    for origem, destino in MAP_ABAS.items():
        fazer_backup(gc, PLANILHA_ORIGEM_ID, PLANILHA_HISTORICO_ID, origem, destino)
        
    print("\n✅ ORQUESTRAÇÃO DE BACKUP CONCLUÍDA.")


if __name__ == "__main__":
    try:
        main()
    except Exception as final_e:
        print(f"\n### FALHA CRÍTICA DO AGENTE ###\n{final_e}")
        sys.exit(1)
