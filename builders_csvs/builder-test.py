from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import random

# Criando sessão Spark
def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

# Gerando dados aleatórios
def generate_random_data(spark, num_records):
    nomes = ["João", "Maria", "Pedro", "Ana", "Miguel", "Sofia", "Lucas", "Laura", "Luís", "Gabriela", "Enzo", "Júlia", "Guilherme", "Isabela", "Rafael", "Manuela", "Gustavo",
             "Valentina", "Henrique", "Helena", "Eduardo", "Larissa", "Vitor", "Beatriz", "Carlos", "Mariana", "Diego", "Bianca", "Fernando", "Camila"]
    
    profissoes = ["Engenheiro", "Professor", "Médico", "Escritor", "Artista", "Programador", "Enfermeiro", "Advogado", "Cozinheiro", "Cientista", "Atleta", "Músico", "Piloto",
                  "Designer", "Psicólogo", "Arquiteto", "Jornalista", "Empresário", "Farmacêutico", "Policial", "Bombeiro", "Veterinário", "Motorista", "Estudante", "Garçom",
                  "Eletricista", "Dentista", "Vendedor", "Padeiro"]
    
    nacionalidades = ["Brasileiro", "Americano", "Britânico", "Japonês", "Alemão", "Francês", "Canadense", "Australiano", "Italiano", "Espanhol", "Português", "Mexicano",
                     "Chinês", "Russo", "Argentino", "Chileno", "Indiano", "Sul-Africano", "Coreano", "Sueco", "Norueguês", "Holandês", "Dinamarquês", "Belga", "Grego",
                     "Israelense", "Turco", "Havaiano", "Peruano"]

    data = []
    for _ in range(num_records):
        nome = random.choice(nomes)
        idade = random.randint(18, 65)
        genero = random.choice(["Masculino", "Feminino"])
        profissao = random.choice(profissoes)
        nacionalidade = random.choice(nacionalidades)
        salario = random.uniform(2500, 50000)
        data.append((nome, idade, genero, profissao, nacionalidade, salario))

    schema = StructType([
        StructField('Nome', StringType(), nullable=False),
        StructField('Idade', IntegerType(), nullable=False),
        StructField('Gênero', StringType(), nullable=False),
        StructField('Profissão', StringType(), nullable=False),
        StructField('Nacionalidade', StringType(), nullable=False),
        StructField('Salário', FloatType(), nullable=False)
    ])

    return spark.createDataFrame(data, schema)

# Salvando dados como arquivo CSV
def save_data_as_csv(df, file_path, file_name):
    df.coalesce(1).write.csv(file_path + '/' + file_name, header=True, mode='overwrite')

# Função principal
def main():
    spark = create_spark_session('Gerador de CSV - Dados Aleatórios')
    num_records = 200
    df = generate_random_data(spark, num_records)
    file_path = 'C:/Users/alef.alves/codigos/dataframes/'
    file_name = 'dados_aleatorios.csv'
    save_data_as_csv(df, file_path, file_name)

if __name__ == '__main__':
    main()
