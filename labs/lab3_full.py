import yaml
import time
import re
import math
import matplotlib.pyplot as plt
from collections import Counter, defaultdict
from pymongo import MongoClient
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
import nltk

nltk.download('stopwords')

# настройки
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)
client = MongoClient(config['db']['host'], config['db']['port'])
col = client[config['db']['name']][config['db']['collection']]
docs = list(col.find())

stemmer = SnowballStemmer("russian")
stop_words = set(stopwords.words('russian'))

# токены и индекс
inverted_index = defaultdict(set)
total_tokens = 0
processing_time = 0
token_lengths = []

print("Начинаю токенизацию и индексацию...")
start_time = time.time()

for doc in docs:
    text = doc['clean_text'].lower()
    raw_tokens = re.findall(r'[а-яёa-z]+', text)
    
    for t in raw_tokens:
        if len(t) > 2 and t not in stop_words:
            # стемминг 
            stem = stemmer.stem(t)
            inverted_index[stem].add(doc['_id']) 
            
            total_tokens += 1
            token_lengths.append(len(t))

end_time = time.time()
processing_time = end_time - start_time
total_text_size_kb = sum(len(d['clean_text'].encode('utf-8')) for d in docs) / 1024

print(f"\n=== Статистика ЛР 3 (Токенизация) ===")
print(f"Количество токенов: {total_tokens}")
if total_tokens > 0:
    print(f"Средняя длина токена: {sum(token_lengths)/total_tokens:.2f}")
print(f"Время выполнения: {processing_time:.4f} сек")
if total_text_size_kb > 0:
    print(f"Скорость: {total_text_size_kb/processing_time:.2f} KB/sec")
else:
    print("Текста слишком мало для замера скорости.")

# ципф
freq_dict = Counter()
for doc in docs:
    text = doc['clean_text'].lower()
    tokens = [stemmer.stem(t) for t in re.findall(r'[а-яёa-z]+', text) if len(t) > 2 and t not in stop_words]
    freq_dict.update(tokens)

sorted_freq = freq_dict.most_common()
ranks = range(1, len(sorted_freq) + 1)
freqs = [c for w, c in sorted_freq]

plt.figure(figsize=(10, 6))
plt.loglog(ranks, freqs, marker=".")
plt.title("Закон Ципфа (Log-Log)")
plt.xlabel("Ранг")
plt.ylabel("Частота")
plt.grid(True)
plt.savefig("zipf_lab3.png")
print("График закона Ципфа сохранен в zipf_lab3.png")

# булев поиск
def boolean_search(query):
    parts = query.split()
    if not parts: return set()
    
    current_set = set()

    first_word = stemmer.stem(parts[0])
    if first_word in inverted_index:
        current_set = inverted_index[first_word].copy()
    
    i = 1
    while i < len(parts):
        op = parts[i]
        if i + 1 >= len(parts): break
        next_word = stemmer.stem(parts[i+1])
        next_set = inverted_index.get(next_word, set())
        
        if op == "AND":
            current_set = current_set.intersection(next_set)
        elif op == "OR":
            current_set = current_set.union(next_set)
        elif op == "NOT":
            current_set = current_set.difference(next_set)
        
        i += 2
        
    return current_set

while True:
    q = input("\nВведите запрос (например: сюжет AND персонажи NOT скучно) или 'exit': ")
    if q == 'exit': break
    res_ids = boolean_search(q)
    print(f"Найдено документов: {len(res_ids)}")
    for rid in list(res_ids)[:3]:
        doc = next((d for d in docs if d['_id'] == rid), None)
        if doc: print(f"- {doc['url']}")