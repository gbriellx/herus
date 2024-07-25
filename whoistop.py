import requests
import re
import pandas as pd
import time
import csv
import logging
from urllib.parse import urlparse
from datetime import datetime

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Variáveis globais para controle de tempo
ATRASO_BASE = 60  # 1 minuto em segundos
ATRASO_PERSONALIZADO = 5  # 5 minutos
CONTADOR_SUCESSO = 0

def consulta_rdap(dominio):
    url = f"https://rdap.registro.br/domain/{dominio}"
    
    try:
        resposta = requests.get(url)
        resposta.raise_for_status()
        return resposta.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro na consulta RDAP para {dominio}: {str(e)}")
        return {"error": str(e)}

def consulta_cnpj(cnpj):
    url = f"https://receitaws.com.br/v1/cnpj/{cnpj}"
    
    try:
        resposta = requests.get(url)
        resposta.raise_for_status()
        return resposta.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro na consulta CNPJ {cnpj}: {str(e)}")
        return {"error": str(e)}

def extract_key_info(rdap_info):
    email_rdap = None
    cpf_cnpj = None
    name = None

    if 'error' in rdap_info:
        return {
            'email_rdap': 'Error',
            'cpf_cnpj': 'Error',
            'name': 'Error'
        }

    for entity in rdap_info.get('entities', []):
        if 'registrant' in entity.get('roles', []):
            vcard = entity.get('vcardArray', [[]])[1]
            name = vcard[1][3] if len(vcard) > 1 and len(vcard[1]) > 3 else 'Not available'
            for public_id in entity.get('publicIds', []):
                if public_id['type'] in ['cpf', 'cnpj']:
                    cpf_cnpj = public_id['identifier']
        
        for sub_entity in entity.get('entities', []):
            vcard = sub_entity.get('vcardArray', [[]])[1]
            for item in vcard:
                if item[0] == 'email':
                    email_rdap = item[3]
                    break

    return {
        'email_rdap': email_rdap if email_rdap else 'Not available',
        'cpf_cnpj': cpf_cnpj if cpf_cnpj else 'Not available',
        'name': name if name else 'Not available'
    }

def format_cnpj_info(info):
    return {
        'nome': info.get('nome', 'Not available'),
        'fantasia': info.get('fantasia', 'Not available'),
        'logradouro': info.get('logradouro', 'Not available'),
        'numero': info.get('numero', 'Not available'),
        'bairro': info.get('bairro', 'Not available'),
        'municipio': info.get('municipio', 'Not available'),
        'uf': info.get('uf', 'Not available'),
        'cep': info.get('cep', 'Not available'),
        'telefone': info.get('telefone', 'Not available'),
        'situacao': info.get('situacao', 'Not available'),
        'capital_social': info.get('capital_social', 'Not available'),
        'email': info.get('email', 'Not available'),
        'qsa': info.get('qsa', [])
    }

def sanitize_cnpj(cnpj):
    return re.sub(r'\D', '', cnpj)

def save_to_csv(dominio, rdap_info, cnpj_info, filename):
    global CONTADOR_SUCESSO
    try:
        df = pd.read_csv(filename)
    except FileNotFoundError:
        df = pd.DataFrame(columns=[
            'Domain', 'Registration Name', 'Trade Name', 'Full Address', 'CEP', 'Phone',
            'Status', 'Social Capital',
            'Partner 1 Name', 'Partner 1 Qualification',
            'Partner 2 Name', 'Partner 2 Qualification',
            'RDAP Email', 'ReceitaWS Email'
        ])

    if dominio in df['Domain'].values:
        logging.info(f"O domínio {dominio} já está na planilha. Não será pesquisado novamente.")
        return False

    qsa = cnpj_info.get('qsa', [])
    new_row = {
        'Domain': dominio,
        'Registration Name': cnpj_info.get('nome', 'Not available'),
        'Trade Name': cnpj_info.get('fantasia', 'Not available'),
        'Full Address': f"{cnpj_info.get('logradouro', 'Not available')}, {cnpj_info.get('numero', 'Not available')}, {cnpj_info.get('bairro', 'Not available')}, {cnpj_info.get('municipio', 'Not available')}, {cnpj_info.get('uf', 'Not available')}",
        'CEP': cnpj_info.get('cep', 'Not available'),
        'Phone': cnpj_info.get('telefone', 'Not available'),
        'Status': cnpj_info.get('situacao', 'Not available'),
        'Social Capital': cnpj_info.get('capital_social', 'Not available'),
        'Partner 1 Name': qsa[0].get('nome', 'Not available') if len(qsa) > 0 else 'Not available',
        'Partner 1 Qualification': qsa[0].get('qual', 'Not available') if len(qsa) > 0 else 'Not available',
        'Partner 2 Name': qsa[1].get('nome', 'Not available') if len(qsa) > 1 else 'Not available',
        'Partner 2 Qualification': qsa[1].get('qual', 'Not available') if len(qsa) > 1 else 'Not available',
        'RDAP Email': rdap_info.get('email_rdap', 'Not available'),
        'ReceitaWS Email': cnpj_info.get('email', 'Not available')
    }

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df.to_csv(filename, index=False, encoding='utf-8')
    logging.info(f"Informações do domínio {dominio} salvas com sucesso.")
    CONTADOR_SUCESSO += 1
    return True

def clean_domains(domains):
    cleaned_domains = []
    for domain in domains:
        domain = domain.strip().lower()
        domain = re.sub(r'^https?://', '', domain)
        domain = domain.rstrip('/')
        domain = re.sub(r'^www\.', '', domain)
        parsed = urlparse('http://' + domain)
        domain_parts = parsed.netloc.split('.')
        if len(domain_parts) > 2:
            domain = '.'.join(domain_parts[-3:])
        else:
            domain = '.'.join(domain_parts)
        if domain.endswith('.br'):
            cleaned_domains.append(domain)
    return cleaned_domains

def main():
    global CONTADOR_SUCESSO
    input_filename = 'dominios.csv'
    output_filename = 'informacoes_empresa.csv'
    
    try:
        with open(input_filename, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)
            domains = [row[0] for row in reader if row]
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo CSV: {e}")
        return
    
    cleaned_domains = clean_domains(domains)
    
    try:
        df_existente = pd.read_csv(output_filename)
        dominios_existentes = set(df_existente['Domain'])
    except FileNotFoundError:
        dominios_existentes = set()
    
    for dominio in cleaned_domains:
        if dominio in dominios_existentes:
            logging.info(f"O domínio {dominio} já está na planilha. Não será pesquisado novamente.")
            continue

        inicio_processamento = time.time()
        logging.info(f"Processando domínio: {dominio}")
        
        rdap_resultado = consulta_rdap(dominio)
        time.sleep(10)  # Espera 10 segundos após a consulta RDAP
        
        if isinstance(rdap_resultado, dict) and 'error' not in rdap_resultado:
            key_info = extract_key_info(rdap_resultado)
            logging.info(f"Domínio: {dominio} - Dados RDAP recuperados com sucesso")
            
            if key_info['cpf_cnpj']:
                cnpj_sanitizado = sanitize_cnpj(key_info['cpf_cnpj'])
                if len(cnpj_sanitizado) == 14:
                    receita_info = consulta_cnpj(cnpj_sanitizado)
                    time.sleep(10)  # Espera 10 segundos após a consulta CNPJ
                    if isinstance(receita_info, dict) and 'error' not in receita_info:
                        formatted_info = format_cnpj_info(receita_info)
                        
                        if save_to_csv(dominio, key_info, formatted_info, output_filename):
                            logging.info(f"Domínio: {dominio} - Informações salvas com sucesso")
                        else:
                            logging.info(f"Domínio: {dominio} - Informações não salvas (já existente ou erro)")
                    else:
                        logging.error(f"Domínio: {dominio} - Erro na consulta ReceitaWS: {receita_info.get('error', 'Erro desconhecido')}")
                else:
                    logging.warning(f"Domínio: {dominio} - O CPF/CNPJ não é um CNPJ válido.")
            else:
                logging.warning(f"Domínio: {dominio} - CNPJ não encontrado na consulta RDAP.")
        else:
            logging.error(f"Domínio: {dominio} - Erro na consulta RDAP: {rdap_resultado.get('error', 'Erro desconhecido')}")
        
        # Calcula o tempo restante para completar 1 minuto
        tempo_processamento = time.time() - inicio_processamento
        tempo_espera = max(0, ATRASO_BASE - tempo_processamento)
        time.sleep(tempo_espera)
        
        # Aplica o atraso personalizado a cada 5 domínios processados com sucesso
        if CONTADOR_SUCESSO > 0 and CONTADOR_SUCESSO % 5 == 0:
            logging.info(f"Aplicando atraso personalizado de {ATRASO_PERSONALIZADO} minutos após {CONTADOR_SUCESSO} domínios processados com sucesso.")
            time.sleep(ATRASO_PERSONALIZADO * 60)
            
if __name__ == "__main__":
    main()

