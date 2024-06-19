from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import random
import os
import glob
import shutil


# Criando sessão spark
def create_spark_session(builder_csv):
    return SparkSession.builder.appName(builder_csv).getOrCreate()


# Gerando dados de forma aleatória
def generate_random_data(spark, num_records):
    data = [(get_random_name(), get_random_age(), get_random_nationality(
    ), get_random_profession(), get_random_salary())for _ in range(num_records)]
    schema = StructType([
        StructField('Name', StringType(), nullable=False),
        # StructField('Idade', IntegerType(), nullable=False),
        StructField('Nacionalidade', StringType(), nullable=False),
        StructField('Profissao', StringType(), nullable=False)
        # StructField('Salario', IntegerType(), nullable=False)

    ])

    return spark.createDataFrame(data, schema)


# Variaveis que podem ser alteradas caso precise para adicionar mais informações ao csv

# Gerador de nomes
def get_random_name():
    names = ["João", "Maria", "Pedro", "Ana", "Miguel", "Sofia", "Lucas", "Laura", "Luís", "Gabriela", "Enzo", "Júlia", "Guilherme", "Isabela", "Rafael", "Manuela", "Gustavo",
             "Valentina", "Henrique", "Helena", "Eduardo", "Larissa", "Vitor", "Beatriz", "Carlos", "Mariana", "Diego", "Bianca", "Fernando", "Camila"]
    return random.choice(names)


# Gerador de idade
def get_random_age():
    return random.randint(18, 65)


# Gerador de nacionalidade
def get_random_nationality():
    nationalities = ["Brasileiro", "Americano", "Britânico", "Japonês", "Alemão", "Francês", "Canadense", "Australiano", "Italiano", "Espanhol", "Português", "Mexicano",
                     "Chinês", "Russo", "Argentino", "Chileno", "Indiano", "Sul-Africano", "Coreano", "Sueco", "Norueguês", "Holandês", "Dinamarquês", "Belga", "Grego",
                     "Israelense", "Turco", "Havaiano", "Peruano"]
    return random.choice(nationalities)


# Gerador de profissao
def get_random_profession():
    profession = ["Engenheiro", "Professor", "Médico", "Escritor", "Artista", "Programador", "Enfermeiro", "Advogado", "Cozinheiro", "Cientista", "Atleta", "Músico", "Piloto",
                  "Designer", "Psicólogo", "Arquiteto", "Jornalista", "Empresário", "Farmacêutico", "Policial", "Bombeiro", "Veterinário", "Motorista", "Estudante", "Garçom",
                  "Eletricista", "Dentista", "Vendedor", "Padeiro"]
    return random.choice(profession)


# Gerador de ssalario
def get_random_salary():
    return random.choice(2500, 50000)


def save_data_as_csv(df, file_path, new_file_name):
    temp_file_path = file_path + '/temp.csv'
    df.coalesce(1).write.csv(temp_file_path, header=True, mode='overwrite')

    # Renomeando o arquivo
    files = glob.glob(temp_file_path + '/part*.csv')
    if len(files) > 0:
        old_file_path = files[0]
        new_file_path = file_path + '/' + new_file_name + '.csv'
        os.rename(old_file_path, new_file_path)

    # Removendo a pasta temp
    shutil.rmtree(temp_file_path)


def main():
    spark = create_spark_session('Gerador CSV random data')
    num_records = 200
    df = generate_random_data(spark, num_records)
    file_path = 'C:/Users/alef.alves/codigos/dataframes'
    new_file_name = "data_profession.csv"
    save_data_as_csv(df, file_path, new_file_name)


if __name__ == '__main__':
    main()
