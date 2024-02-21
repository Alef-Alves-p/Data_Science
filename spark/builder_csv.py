import pandas as pd
import numpy as np
import names
import random
import pycountry


def generate_random_data(num_rows):
    names_list = [names.get_full_name() for _ in range(num_rows)]
    ages = np.random.randint(18, 65, size=num_rows)
    nationalities = [generate_random_country() for _ in range(num_rows)]
    professions = ["Engenheiro", "Professor", "Médico", "Escritor", "Artista", "Programador", "Enfermeiro", "Advogado", "Cozinheiro", "Cientista", "Atleta", "Músico", "Piloto",
                  "Designer", "Psicólogo", "Arquiteto", "Jornalista", "Empresário", "Farmacêutico", "Policial", "Bombeiro", "Veterinário", "Motorista", "Estudante", "Garçom",
                  "Eletricista", "Dentista", "Vendedor", "Padeiro"]
    salaries = np.random.uniform(2000, 10000, size=num_rows)
    
    data = {
        'Nome': names_list,
        'Idade': ages,
        'Nacionalidade': nationalities,
        'Profissão': [random.choice(professions) for _ in range(num_rows)],
        'Salário': salaries
    }
    return pd.DataFrame(data)


def generate_random_country():
    country = random.choice(list(pycountry.countries))
    return country.name


def save_data_as_csv(data, file_path):
    data.to_csv(file_path, index=False)


def main():
    # Configurar o número de linhas, o caminho da pasta e o nome do arquivo
    num_rows = 1000
    folder_path = 'C:\\Users\\alef.alves\\codigos\\dataframes'
    file_name = 'df_profissoes.csv'

    # Gerar os dados aleatórios
    random_data = generate_random_data(num_rows)

    # Salvar os dados como CSV
    file_path = f'{folder_path}/{file_name}'
    save_data_as_csv(random_data, file_path)


if __name__ == '__main__':
    main()
