#pip install pyodbc pysentimiento nltk pandas joblib tqdm

import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm
import limpiarData
from conexionBd import Database
import configparser
from datetime import datetime

# Habilitar el uso de tqdm con pandas
tqdm.pandas()

# Crear un objeto ConfigParser
config = configparser.ConfigParser()
config.read('config.ini')

db = Database()

# Define la ruta a tu archivo CSV
ruta = r"D:\seminario\proyecto final\python-tweets_turismo_226K.csv"

chunks = []
modo_prueba_str = config['default']['modo_prueba']
modo_prueba = modo_prueba_str.lower() == 'true'
try:
    if modo_prueba:
        # Leer solo las primeras `limite_filas` filas del archivo CSV
        df_pandas = pd.read_csv(config['default']['rutaCargueArchivo'], usecols=['date', 'content'], delimiter=',', on_bad_lines='skip', encoding='utf-8', nrows=int(config['default']['limite_filas']))
    else:
        # Configurar tqdm para lectura de fragmentos
        for chunk in tqdm(pd.read_csv(config['default']['rutaCargueArchivo'], usecols=['date', 'content'], delimiter=',', on_bad_lines='skip', encoding='utf-8', chunksize=int(config['default']['chunk_size'])),
                          desc="Leyendo fragmentos", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
            chunks.append(chunk)
        df_pandas = pd.concat(chunks, ignore_index=True)

    # Validar las fechas y filtrar
    df_pandas = df_pandas[df_pandas['date'].progress_apply(lambda date, **kwargs: limpiarData.is_valid_date(date), 
                                                           desc="Validando fechas", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}')]

    # Limpiar y filtrar el contenido original
    df_pandas['cleaned_content'] = df_pandas['content'].astype(str).progress_apply(
        lambda x, **kwargs: limpiarData.clean_advertising(x) if isinstance(x, str) else '',
        desc="Limpiando contenido",
        bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'
    )
    df_pandas = df_pandas[df_pandas['cleaned_content'].progress_apply(
        lambda x, **kwargs: isinstance(x, str) and x.strip() != '',
        desc="Filtrando contenido vacío",
        bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'
    )]

    # Filtrar spam
    df_pandas = df_pandas[df_pandas['cleaned_content'].progress_apply(
        lambda x, **kwargs: limpiarData.filter_spam(x, limpiarData.keywords_to_exclude),
        desc="Filtrando spam",
        bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'
    )]

    # Eliminar la columna original 'content'
    df_pandas.drop(columns=['content'], inplace=True)

    # Definir función para análisis de sentimiento en paralelo
    def analyze_sentiment_in_parallel(text):
        return limpiarData.analyze_sentiment(text)

    # Aplicar análisis de sentimiento en paralelo
    sentiments = Parallel(n_jobs=-1)(delayed(analyze_sentiment_in_parallel)(text) for text in tqdm(df_pandas['cleaned_content'], desc="Analizando sentimientos"))

    df_pandas['sentiment'] = sentiments
    df_pandas['date'] = df_pandas['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S'))
    
    print(df_pandas.head(10))  # Para verificar el resultado

    # Insertar datos en la base de datos
    for index, row in df_pandas.iterrows():
        date = row['date']
        cleaned_content = row['cleaned_content']
        sentiment = row['sentiment']
        
        # Construir y ejecutar la consulta de inserción
        query = "INSERT INTO TweetsSentiments (fecha, texto, sentimiento) VALUES (?, ?, ?)"
        params = (date, cleaned_content, sentiment)
        
        # Ejecutar la consulta
        db.execute_query(query, params)

except pd.errors.ParserError as e:
    print(f"Error al analizar el archivo CSV: {e}")
except Exception as e:
    print(f"Ocurrió un error: {e}")
finally:
    db.close()
