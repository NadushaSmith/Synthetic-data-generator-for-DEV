# -*- coding: utf-8 -*-
import random
import string
import os
import shutil
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import lit

# Функция генерации name состоящие от 5 до 12 символов, создается по условию: четные согласные, нечетные гласные
def generation_random_name(min_length=5):
  length = random.randint(min_length, 12)
  vowels = 'aeiou'
  constants = ''.join(set(string.ascii_lowercase) - set(vowels))
  name = ''
  for i in range(length):
    if i % 2 == 0:
      name += random.choice(constants)
    else:
      name += random.choice(vowels)
  return name.capitalize()

# Функция генерации emal на основе имени (name)
def generation_email(name):
    domains = ['@mail.ru', '@gmail.com', '@yandex.ru', '@company.com' ]
    return name.lower() + str(random.randint(1, 99)) + random.choice(domains)

# Функция генерации city

def generation_city(min_length = 7):
  cities = ['Moscow', 'Sant Petersburg', 'Kazan', 'Omsk', 'Rostov-on-Don', 'Ufa', 'Krasnoyarsk', 'Samara', 'Irkutsk', 'Novosibirsk', 'London', 'New York', 'Nizhny Novgorod']
  return random.choice([city for city in cities if len(city) >= min_length])

# Функция генерации даты регистарации

def generation_registration_date(age):
  current_date = datetime.now()
  birth_year = current_date.year - age
  min_reg_date = datetime(birth_year + 18, 1, 1)
  max_reg_date = current_date
  delta = (max_reg_date - min_reg_date).days

  random_days = random.randint(0, delta)
  return (min_reg_date + timedelta(days=random_days)).date()

# Функция генерации данных согласно структуре
def generation_dataset(num_rows):
  data = []

  for i in range(1, num_rows + 1 ):
    name = generation_random_name()
    email = generation_email(name)
    city = generation_city()
    age = random.randint(18, 95)
    salary = round(random.uniform(30000, 300000), 2)
    registration_date = generation_registration_date(age)

    if random.random() < 0.05:
      city = None
    if random.random() < 0.05:
      salary = None
    if random.random() < 0.05:
      registration_date = None

    data.append((i, name, email, city, age, salary, registration_date))
  return data
# Функция main() итоговая генерация данных

def main():
  num_rows = int(input())
  spark = SparkSession.builder.appName('DatagenerationTest').getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  data = generation_dataset(num_rows)

# Определяем схему данных
  schema = StructType([
      StructField('id', IntegerType(), False),
      StructField('name', StringType(), False),
      StructField('email', StringType(), False),
      StructField('city', StringType(), True),
      StructField('age', IntegerType(), False),
      StructField('salary', DoubleType(), True),
      StructField('registration_date', DateType(), True)
  ])

# Создаем DataFrame
  df = spark.createDataFrame(data, schema=schema)

# генерируем имя файла ('2025-07-23.csv')
  current_date = datetime.now().strftime('%Y-%m-%d') # возвращаем текущую дату и форматируем под год-месяц-день
  output_filename = f"{current_date}-dev" # форматируем имя файла текущая дата и добавим dev

# Сохраняем данные в файл csv

  (df.coalesce(1).write # соберем все патиции в один раздел и сохраним датафрейм в файл
    .mode('overwrite') # перезапись данных
    .option('header', 'true')  # включим запись заголовков с именами столбцов
    .option('nullValue', 'NULL') # задаем значение NULL если данных нет, в строке будет NULL
    .csv(output_filename)) # сохраним данные в csv формате



# поиск csv-файла в директории
  csv_file = [f for f in os.listdir(output_filename) # получаем список всех файлов в директории
          if f.startswith('part') and f.endswith('.csv')] # добавляем фильтры к поиску, файл начинается path и проверяем расширение csv, берем первый найденный[0]

  if not csv_file:
      raise FileNotFoundError(f"No CSV file found in {output_filename}")

  src_path = os.path.join(output_filename, csv_file[0])
  dest_path = f"{output_filename}.csv"


  shutil.move(src_path, dest_path)


  try:
    os.rmdir(output_filename)
  except OSError:
      shutil.rmtree(output_filename)


      print(f"Data successfully saved to {dest_path}")


      spark.stop()

if __name__ == "__main__":

    main()
