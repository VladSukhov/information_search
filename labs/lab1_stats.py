import yaml
from pymongo import MongoClient
import sys

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

client = MongoClient(config['db']['host'], config['db']['port'])
col = client[config['db']['name']][config['db']['collection']]

docs = list(col.find())
total_docs = len(docs)

if total_docs == 0:
    print("База пуста! Запустите crawler.py")
    sys.exit()

raw_size_bytes = sum(len(d['raw_html'].encode('utf-8')) for d in docs)
clean_size_bytes = sum(len(d['clean_text'].encode('utf-8')) for d in docs)

print("=== Результаты ЛР 1 ===")
print(f"Количество документов: {total_docs}")
print(f"Размер 'сырых' данных (HTML): {raw_size_bytes / 1024:.2f} KB")
print(f"Размер выделенного текста: {clean_size_bytes / 1024:.2f} KB")
print(f"Средний размер документа (HTML): {raw_size_bytes / total_docs:.2f} bytes")
print(f"Средний объем текста: {clean_size_bytes / total_docs:.2f} bytes")