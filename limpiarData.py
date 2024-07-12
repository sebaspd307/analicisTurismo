import re
import nltk
from nltk.corpus import stopwords
import string
from pysentimiento import create_analyzer


# Función para verificar si una fecha es válida
date_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{2}:\d{2}"

# Configurar stopwords y patrones de expresión regular fuera de la función UDF
nltk.download('stopwords')
stopWords = set(stopwords.words('spanish'))
pattern = r'''(?x)                    # set flag to allow verbose regexps
              (?:[A-Z]\.)+            # abbreviations, e.g. U.S.A
              | \w+(?:-\w+)*          # Words with optional internal hyphens
              | \$?\d+(?:\.\d+)?%?    # Currency and percentages, e.g. $12.40 82%
              | \.\.\.                # Ellipsis
              | [][.,;"'?():-_`]      # These are separate tokens; includes ],[
              '''
re_punc = re.compile('[%s]' % re.escape(string.punctuation))

def is_valid_date(date, **kwargs):
    date_pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{2}:\d{2}$'
    if isinstance(date, str):
        return bool(re.match(date_pattern, date))
    return False

def clean_advertising(text, **kwargs):
    text = str(text)
    text = re.sub(r'@[A-Za-z0-9]+', ' ', text)  # Remover menciones @
    text = re.sub(r'RT[|\s]', ' ', text)  # Remover RTs
    text = re.sub(r'#', ' ', text)  # Remover # en el tweet
    text = re.sub(r'https?:\/\/\S+', ' ', text)  # Remover links
    text = text.lower()
    words = nltk.regexp_tokenize(text, pattern)
    stripped = [re_punc.sub('', w) for w in words]
    no_garbage = [w for w in stripped if w.lower() not in stopWords]  # Remover stopwords
    return " ".join(no_garbage)

# Función para filtrar spam
keywords_to_exclude = ["oferta", "promoción", "descuento", "rebaja", "liquidación",
                       "ganga", "comprar", "venta", "precio especial", "outlet",
                       "¡Aprovecha!", "¡Compra ya!", "Últimos días", "Solo por hoy",
                       "gratis", "regalo", "sorteo", "ganador", "spam", "clickbait",
                       "phishing", "scam", "viaja con"]

def filter_spam(text, keywords_to_exclude, **kwargs):
    text_lower = text.lower()
    for keyword in keywords_to_exclude:
        if keyword in text_lower:
            return False  # Es spam
    return True  # No es spam

# Función para el análisis de sentimiento
def analyze_sentiment(text, **kwargs):
    analyzer = create_analyzer(task="sentiment", lang="es")  # Cambia "es" por el idioma que necesites
    result = analyzer.predict(text)
    return result.output  # Esto devuelve 'POS', 'NEG', o 'NEU'
