import threading
import queue
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from collections import Counter
import time
import json
import re
from dataclasses import dataclass, asdict
from typing import Set, List, Dict
from requests.exceptions import Timeout, ConnectionError, RequestException

# Конфигурация
START_URL = "https://lenta.ru/"
MAX_DEPTH = 3
TIMEOUT = 10
THREADS = 5
OUTPUT_FILE = "lenta_analysis.json"
DELAY_BETWEEN_REQUESTS = 0.5

# Стоп-слова
STOP_WORDS = set("""
и в на с к по не он она оно они быть это как так но да же вот у о
а за из от до при для без через об то всё все весь вся весь
этот та тех тех тот те та же ещё ещё уже потом потом потом
который которая которое которые потому поэтому
""".split())

# ХРАНЕНИЕ ДАННЫХ
visited = set()
visited_lock = threading.Lock()


@dataclass
class PageData:
    url: str
    depth: int
    title: str
    text_preview: str
    word_count: int
    status_code: int
    error: str = ""


all_pages: List[PageData] = []
pages_lock = threading.Lock()
word_counter = Counter()
word_counter_lock = threading.Lock()


# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
def is_same_domain(url: str, base_domain: str) -> bool:
    # Проверяет, принадлежит ли URL тому же домену
    try:
        parsed = urlparse(url)
        return parsed.netloc == base_domain
    except:
        return False


def clean_text(html: str) -> str:
    # Извлекает чистый текст из HTML
    soup = BeautifulSoup(html, 'html.parser')
    # Удаляет скрипты, стили и теги с неинформативным содержимым
    for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'meta', 'link']):
        tag.decompose()
    text = soup.get_text(separator=' ')
    # Очищает: только буквы и пробелы
    text = re.sub(r'[^а-яёa-z\s]', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+', ' ', text).strip().lower()
    return text


def extract_words(text: str) -> List[str]:
    # Разбивает текст на слова, отфильтровывая стоп-слова и слишком короткие слова
    words = text.split()
    filtered = []
    for w in words:
        if len(w) <= 2:
            continue
        if w in STOP_WORDS:
            continue
        filtered.append(w)
    return filtered


def get_title(soup: BeautifulSoup) -> str:
    # Извлекает заголовок страницы
    title_tag = soup.find('title')
    if title_tag:
        return title_tag.get_text(strip=True)[:200]
    h1 = soup.find('h1')
    if h1:
        return h1.get_text(strip=True)[:200]
    return "Без заголовка"


def extract_internal_links(html: str, base_url: str, current_depth: int, base_domain: str) -> Set[str]:
    # Извлекает все внутренние ссылки со страницы
    soup = BeautifulSoup(html, 'html.parser')
    links = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        full_url = urljoin(base_url, href)
        # Очищает якоря и параметры для уникальности
        full_url = urlparse(full_url)._replace(fragment="").geturl()
        if is_same_domain(full_url, base_domain) and current_depth < MAX_DEPTH:
            if not any(skip in full_url for skip in ['#', 'javascript:', 'mailto:', 'tel:']):
                links.add(full_url)
    return links


# ФУНКЦИЯ ОБРАБОТКИ ОДНОЙ СТРАНИЦЫ
def process_page(url: str, depth: int, base_domain: str, session: requests.Session):
    # Загружает, парсит и анализирует одну страницу
    # Инициализация всех переменных
    status = 0
    text_content = ""
    title = ""
    word_count = 0
    error_msg = ""
    response = None
    html = ""

    try:
        response = session.get(url, timeout=TIMEOUT)
        status = response.status_code

        if status == 200:
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' not in content_type:
                error_msg = f"Не HTML: {content_type}"
            else:
                response.encoding = 'utf-8'
                html = response.text
                soup = BeautifulSoup(html, 'html.parser')
                title = get_title(soup)
                raw_text = clean_text(html)
                words = extract_words(raw_text)
                word_count = len(words)
                text_content = ' '.join(words[:500])

                with word_counter_lock:
                    word_counter.update(words)
        else:
            error_msg = f"HTTP {status}"

    except requests.exceptions.Timeout:
        error_msg = "Таймаут"
        status = 0
    except requests.exceptions.ConnectionError:
        error_msg = "Ошибка соединения"
        status = 0
    except requests.exceptions.RequestException as e:
        error_msg = f"Ошибка запроса: {str(e)}"
        status = 0
    except Exception as e:
        error_msg = str(e)
        status = 0

    # Сохраняет результат
    page_data = PageData(
        url=url,
        depth=depth,
        title=title[:200] if title else "Без заголовка",
        text_preview=(text_content[:300] if text_content else "Нет текста"),
        word_count=word_count,
        status_code=status,
        error=error_msg
    )
    with pages_lock:
        all_pages.append(page_data)

    # Возвращает ссылки только при успешной загрузке
    if status == 200 and not error_msg and response and html:
        try:
            links = extract_internal_links(html, url, depth + 1, base_domain)
            return links
        except Exception as e:
            print(f"Ошибка извлечения ссылок с {url}: {e}")
            return set()
    return set()


# ПОТОКОВЫЙ ОБХОДЧИК
def worker(task_queue: queue.Queue, base_domain: str, session: requests.Session,
           stop_event: threading.Event, progress: Dict):
    # Рабочий поток
    while not stop_event.is_set():
        try:
            url, depth = task_queue.get(timeout=1)
        except queue.Empty:
            break

        # Проверяет, не посещали ли уже страницу
        with visited_lock:
            if url in visited:
                task_queue.task_done()
                continue
            visited.add(url)

        # Обновляет прогресс
        progress['processing'] = url
        progress['visited'] = len(visited)
        progress['queue'] = task_queue.qsize()
        print(f"[{progress['visited']}] Глубина {depth}: {url[:80]}...")

        # Обрабатывает страницу
        new_links = process_page(url, depth, base_domain, session)

        # Добавляет новые ссылки в очередь
        with visited_lock:
            for link in new_links:
                if link not in visited:
                    task_queue.put((link, depth + 1))

        task_queue.task_done()
        time.sleep(DELAY_BETWEEN_REQUESTS)


def crawl(start_url: str, max_depth: int = 3, threads: int = 5):
    # Запускает многопоточный обход сайта
    parsed = urlparse(start_url)
    base_domain = parsed.netloc

    task_queue = queue.Queue()
    task_queue.put((start_url, 0))

    progress = {'processing': '', 'visited': 0, 'queue': 1}
    stop_event = threading.Event()

    # Создаёт сессии с общими заголовками
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })

    # Запускает потоки
    workers = []
    for _ in range(threads):
        t = threading.Thread(target=worker, args=(task_queue, base_domain, session, stop_event, progress))
        t.start()
        workers.append(t)

    # Ожидает завершения
    try:
        task_queue.join()
    except KeyboardInterrupt:
        print("\n[!] Прервано пользователем")
        stop_event.set()

    for t in workers:
        t.join(timeout=2)

    session.close()
    print(f"\nОбход завершён. Обработано страниц: {len(visited)}")


# АНАЛИЗ И ВЫВОД
def print_top_words(counter: Counter, top_n: int = 10):
    # Выводит топ-N самых частых слов
    print(f"ТОП-{top_n} САМЫХ ЧАСТЫХ СЛОВ (исключены стоп-слова, слова короче 3 букв):")
    most_common = counter.most_common(top_n)
    for i, (word, count) in enumerate(most_common, 1):
        print(f"{i:2}. {word:15} → {count:5} раз")
    return most_common


def save_results():
    # Сохраняет результаты в JSON
    output = {
        "start_url": START_URL,
        "max_depth": MAX_DEPTH,
        "pages_visited": len(all_pages),
        "top_10_words": [{"word": w, "count": c} for w, c in word_counter.most_common(10)],
        "pages": [asdict(p) for p in all_pages]
    }
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\nРезультаты сохранены в {OUTPUT_FILE}")


# ГЛАВНАЯ
if __name__ == "__main__":
    print(f"Запуск обхода: {START_URL}")
    print(f"Глубина обхода: {MAX_DEPTH}")
    print(f"Потоков: {THREADS}\n")

    start_time = time.time()
    crawl(START_URL, MAX_DEPTH, THREADS)
    elapsed = time.time() - start_time

    # Анализ и вывод результатов
    print_top_words(word_counter, 10)
    save_results()

    print(f"\nВремя выполнения: {elapsed:.2f} сек")
    print(f"Всего страниц с текстом: {len([p for p in all_pages if p.word_count > 0])}")