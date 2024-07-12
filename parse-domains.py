import os
import csv
import aiohttp
import aiofiles
import asyncio
from lxml import html
from urllib.parse import urlparse, urljoin
from asyncio import Queue, QueueEmpty
import socket
import platform
from typing import Optional, Dict, List

# Встановити SelectorEventLoop для Windows
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Параметри
TIMEOUT = 10
NUM_THREADS = 20
MAX_RETRIES = 0
MAX_REDIRECTS = 1
LOG_LEVEL = "info"
CHUNK_SIZE = 7000  # Зменшити розмір порції для обробки

# Глобальна подія для завершення потоків
shutdown_event = asyncio.Event()

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Referer': 'http://www.google.com'
}

async def get_ip_address(domain: str) -> Optional[str]:
    loop = asyncio.get_event_loop()
    try:
        ip_address = await loop.getaddrinfo(domain, None)
        return ip_address[0][4][0] if ip_address else None
    except socket.error as e:
        #print(f"Error resolving IP for {domain}: {e}")
        return None

def is_same_domain(url: str, base_url: str) -> bool:
    base_domain = urlparse(base_url).netloc
    redirect_domain = urlparse(url).netloc
    return redirect_domain.endswith(base_domain)

async def fetch(session: aiohttp.ClientSession, url: str, retries: int) -> tuple[Optional[bytes], Optional[int], Optional[aiohttp.ClientResponse]]:
    try:
        async with session.get(url, timeout=TIMEOUT, headers=HEADERS, allow_redirects=False) as response:
            raw_response = await response.read()
            return raw_response, response.status, response.headers
    except (aiohttp.ClientError, aiohttp.ClientConnectorError, asyncio.TimeoutError, ConnectionResetError) as e:
        if retries > 0:
            #print(f"Retrying {url} due to {e}. Retries left: {retries}")
            await asyncio.sleep(1)
            return await fetch(session, url, retries - 1)
        #print(f"Failed to fetch {url} after retries: {e}")
        return None, None, None

async def process_domain(domain: str, result_file_locks: Dict[str, asyncio.Lock]):
    #print(f"Processing domain: {domain}")
    protocols = ["https://www.", "https://", "http://", "http://www."]
    final_response = None
    ip_address = await get_ip_address(domain)
    if ip_address is None:
        print(f"{domain} немає IP")
        await save_results_buffered([{
            "Domain": domain,
            "Final Response": None,
            "IP Address": None,
            "Custom Search Keys": None,
            "lang": None,
            "hreflang": None,
        }], "result_non_IP.csv", ["Domain"], result_file_locks["result_non_IP.csv"])
        return

    custom_search_keys = {}
    lang = None
    hreflangs = []

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False, limit_per_host=NUM_THREADS)) as session:
        for protocol in protocols:
            url = protocol + domain
            base_url = url
            redirect_count = 0
            while redirect_count < MAX_REDIRECTS:
                response_text, status, headers = await fetch(session, url, MAX_RETRIES)
                if status == 200 and response_text:
                    final_response = status
                    break
                elif status in [301, 302, 303, 307, 308]:
                    redirect_url = headers.get('Location')
                    if redirect_url and not redirect_url.startswith("http"):
                        redirect_url = urljoin(url, redirect_url)
                    if redirect_url and is_same_domain(redirect_url, base_url):
                        url = redirect_url
                        redirect_count += 1
                    else:
                        break
                elif status in [401, 403]:
                    break
                else:
                    break
            if final_response == 200:
                break

        if final_response != 200 or not response_text:
            await save_results_buffered([{
                "Domain": domain,
                "Final Response": final_response,
                "IP Address": ip_address,
                "Custom Search Keys": None,
                "lang": None,
                "hreflang": None,
            }], "result_non_200.csv", ["Domain"], result_file_locks["result_non_200.csv"])
            return

        cleaned_html = response_text.decode('utf-8', errors='ignore').lower()

        search_key_phrases = ["woocommerce", "/cart", "/product", "/shop",  "/checkout", "product", "add-to-cart", "checkout", "cart", "shop", "store"]

        for phrase in search_key_phrases:
            count = cleaned_html.count(phrase)
            if count > 0:
                custom_search_keys[phrase] = count

        tree = html.fromstring(response_text)
        html_tag = tree.xpath('//html')
        if html_tag:
            lang = html_tag[0].get('lang')

        for link in tree.xpath("//link[@rel='alternate' and @hreflang]"):
            hreflangs.append(link.attrib['hreflang'])

    result = {
        "Domain": domain,
        "Final Response": final_response,
        "IP Address": ip_address,
        "Custom Search Keys": "||".join([f"{k}:{v}" for k, v in custom_search_keys.items()]),
        "lang": lang,
        "hreflang": "||".join(hreflangs),
    }

    await save_results_buffered([result], "result_200.csv", list(result.keys()), result_file_locks["result_200.csv"])

    if final_response == 200:
        print(f"домен {domain} отримав кінцеву відповідь {final_response}, IP: {ip_address}, Custom Search Keys: {custom_search_keys}, lang: {lang}, hreflangs: {hreflangs}")

async def worker(queue: Queue, result_file_locks: Dict[str, asyncio.Lock]):
    try:
        while not shutdown_event.is_set():
            try:
                domain = await queue.get()
                await process_domain(domain, result_file_locks)
                queue.task_done()
            except QueueEmpty:
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in worker: {e}")
    except GeneratorExit:
        pass

async def save_results_buffered(results_buffer: List[dict], filename: str, fieldnames: List[str], result_file_lock: asyncio.Lock):
    async with result_file_lock:
        file_exists = os.path.isfile(filename)
        async with aiofiles.open(filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                await f.write(f"{','.join(fieldnames)}\n")
            for result in results_buffer:
                await f.write(f"{','.join([str(result[field]) for field in fieldnames])}\n")

async def process_chunk(domains_chunk: List[str]):
    queue = Queue()
    for domain in domains_chunk:
        await queue.put(domain)

    result_file_locks = {
        "result_non_IP.csv": asyncio.Lock(),
        "result_200.csv": asyncio.Lock(),
        "result_non_200.csv": asyncio.Lock(),
    }

    tasks = []

    for _ in range(NUM_THREADS):
        task = asyncio.create_task(worker(queue, result_file_locks))
        tasks.append(task)

    await queue.join()

    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

async def main():
    async with aiofiles.open("all-woocommerce-domains.csv", 'r', encoding='utf-8') as f:
        domains_chunk = []
        async for line in f:
            domain = line.strip()
            domains_chunk.append(domain)
            if len(domains_chunk) >= CHUNK_SIZE:
                await process_chunk(domains_chunk)
                domains_chunk = []

        # Process any remaining domains
        if domains_chunk:
            await process_chunk(domains_chunk)

if __name__ == "__main__":
    asyncio.run(main())
