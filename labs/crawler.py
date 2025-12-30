import time
import yaml
import json
import requests
import os
import re
from datetime import datetime
from pymongo import MongoClient, errors
from bs4 import BeautifulSoup

# конфигурации
if os.path.exists('config.yaml'):
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
else:
    config = {
        'db': {'host': 'localhost', 'port': 27017, 'name': 'ir_lab', 'collection': 'documents'},
        'logic': {'delay': 2.0, 'reindex_days': 7}
    }

# MONGODB
try:
    client = MongoClient(config['db']['host'], config['db']['port'], serverSelectionTimeoutMS=2000)
    db = client[config['db']['name']]
    collection = db[config['db']['collection']]
    client.server_info()
    print("Подключение к MongoDB успешно.")
except errors.ServerSelectionTimeoutError:
    print("ОШИБКА: Не удалось подключиться к MongoDB. Убедитесь, что база запущена (docker/service).")
    exit(1)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
}

STATE_FILE = 'crawler_state.json'

# функции состояния
def get_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {"kanobu": 1}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

# сохранение в БД
def upsert_document(url, raw_html, title, text_content):
    now = time.time()
    
    # Проверка на актуальность
    existing = collection.find_one({"url": url})
    if existing:
        last_date = existing.get('timestamp', 0)
        if now - last_date < config['logic']['reindex_days'] * 86400:
            print(f"[SKIP] Актуально: {url}")
            return

    doc = {
        "url": url,
        "source": "kanobu",
        "raw_html": raw_html,
        "clean_text": text_content,
        "title": title,
        "timestamp": now,
        "date_str": datetime.now().isoformat()
    }
    
    try:
        collection.replace_one({"url": url}, doc, upsert=True)
        print(f"[SAVED] {title[:30]}...")
    except Exception as e:
        print(f"Ошибка записи в БД: {e}")

# парсинг
def parse_article(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            print(f"Ошибка загрузки статьи {r.status_code}")
            return
        
        soup = BeautifulSoup(r.text, 'html.parser')
        
        title_elem = soup.find('h1')
        title = title_elem.get_text(strip=True) if title_elem else "No Title"
        
        content_body = None
        
        candidates = [
            "c-entry__body",
            "article__body",
            "content-body", 
            "js-mediator-article", 
            "article-content", 
            "c-editor-content",
            "doc__body",
            "post-content"
        ]
        
        # поиск по itemprop
        content_body = soup.find(attrs={"itemprop": "articleBody"})
        
        # поиск по классам
        if not content_body:
            for cls in candidates:
                content_body = soup.find(class_=cls)
                if content_body:
                    break
        
        # ищем тег article
        if not content_body:
            content_body = soup.find('article')

        # собираем все длинные параграфы на странице
        text = ""
        if content_body:
            for script in content_body(["script", "style", "iframe", "figure"]):
                script.decompose()
            text = content_body.get_text(separator=" ", strip=True)
        else:
            print(f"[DEBUG] Контейнер не найден, использую эвристику для {url}")
            paragraphs = [p.get_text(strip=True) for p in soup.find_all('p') if len(p.get_text(strip=True)) > 50]
            text = " ".join(paragraphs)

        if text and len(text) > 100:
            upsert_document(url, str(soup), title, text)
        else:
            print(f"[WARN] Не удалось найти текст статьи (или он слишком короткий): {url}")

    except Exception as e:
        print(f"Ошибка парсинга статьи {url}: {e}")
# парсинг списка
def fetch_kanobu_catalog(page):
    url = f"https://kanobu.ru/anime-reviews/?page={page}"
    print(f"\n--- Загрузка каталога: {url} ---")
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 404:
            print("Страница каталога не найдена (404). Похоже, это конец списка.")
            return False
        if r.status_code != 200:
            print(f"Ошибка доступа к каталогу: {r.status_code}")
            return False

        soup = BeautifulSoup(r.text, 'html.parser')
        
        # умныйпоиск ссылок
        all_links = soup.find_all('a', href=True)
        
        found_urls = set()
        for link in all_links:
            href = link['href']
            if '/reviews/' in href and '#comments' not in href:
                if href.startswith('/'):
                    href = "https://kanobu.ru" + href
                
                if 'anime-reviews' in href:
                    continue
                    
                found_urls.add(href)
        
        if not found_urls:
            print("На странице не найдено ссылок на рецензии.")
            if page == 1:
                print("DEBUG: Вывод HTML для анализа...")
                print(soup.prettify()[:1000])
            return False

        print(f"Найдено уникальных ссылок: {len(found_urls)}")
        
        # обход статей
        count = 0
        for article_url in found_urls:
            parse_article(article_url)
            count += 1
            time.sleep(config['logic']['delay'])
            
        return True

    except Exception as e:
        print(f"Критическая ошибка каталога: {e}")
        return False

def run():
    state = get_state()
    if "kanobu" not in state:
        state["kanobu"] = 1
        
    try:
        while True:
            success = fetch_kanobu_catalog(state['kanobu'])
            
            if success:
                state['kanobu'] += 1
                save_state(state)
            else:
                print("Обкачка завершена.")
                break
                
    except KeyboardInterrupt:
        print("\nРобот остановлен пользователем. Прогресс сохранен.")
        save_state(state)

if __name__ == "__main__":
    run()