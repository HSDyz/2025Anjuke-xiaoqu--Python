"""
Created on 2025/11/15
@Author: YZ
Modified:
    1. 动态获取区域列表和价格分段，不再硬编码。
    2. 加强验证码逻辑、添加经纬度获取、断点重爬、自定义爬取。
    3. 加入获取页面范围逻辑不再需要手动设置范围。
    4. 加入全流程，可以直接设置完所有任务，不需要再更换链接.
    5. 使用时只需配置好mongodb设置、COMMON_BASE_URL
    6. 遇到链接验证之后即可继续接着爬取
"""
import requests
import time
import random
import re
import logging
from pyquery import PyQuery as pq
from pymongo import MongoClient
import datetime
from urllib.parse import urlparse
import webbrowser
import json
import os
import sys
from typing import List, Dict, Tuple, Optional

# --- 日志配置 ---
LOG_FILE = "anjuke_crawler.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# --- 核心配置区 ---
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'Anjuke'  # 数据库连接名
COLLECTION_NAME = 'xiaoqu'  # 数据库集合名
CHECKPOINT_FILE = "anjuke_check.json"  # 断点保存记录文件
PAGE_SIZE = 25  # 每页小区数量，固定不改
BATCH_INSERT_SIZE = 50  # 批量插入大小，不改
RETRY_TIMES = 3  # 网络请求重试次数
REGIONS_PRICES_FILE = "regions_prices.json"  # 存储动态获取的区域和价格信息
DEBUG_HTML_DIR = "debug_html"  # 用于保存调试HTML的目录

# --- 公共配置 ---
# 城市页面
COMMON_BASE_URL = "https://chongqing.anjuke.com/community"   # 爬取城市主链接，需要改

# --- 自定义起始爬取配置，开启后不从头开始爬取 (使用动态获取的名称和ID) ---
ENABLE_CUSTOM_START = True  #  自定义开始位置开关，False 关闭
CUSTOM_START_REGION_NAME = '云阳'  # 对应实际区域名称
CUSTOM_START_PRICE_ID = 'm3094'    # 对应价格分段ID
CUSTOM_START_PAGE = 3    # 第几个页面（无需担心第几个小区，自动覆盖）

# --- 代理配置 ---
USE_PROXY = False
PROXY_POOL = [
    # 'http://127.0.0.1:8888',
    # 'http://username:password@proxy_ip:port'
]

# --- 初始化 ---
# MongoDB客户端
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
collection.create_index('url', unique=True)
collection.create_index('community_id')
collection.create_index('region_name')     # 索引区域名称
collection.create_index('price_segment')

# 请求会话
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Referer': COMMON_BASE_URL,
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    #'Cookie': '(按需填写加强反爬,可以不填)',
})

batch_cache = []

# --- 工具函数 ---
def get_proxy() -> Optional[str]:
    if USE_PROXY and PROXY_POOL:
        return random.choice(PROXY_POOL)
    return None

def safe_text(doc, selector):
    elem = doc(selector)
    return elem.text().strip() if elem else None

def get_page(url, timeout=15) -> Optional[str]:
    proxies = {'http': get_proxy(), 'https': get_proxy()} if get_proxy() else None
    for attempt in range(RETRY_TIMES):
        try:
            r = session.get(url, timeout=timeout, proxies=proxies)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or 'utf-8'

            # 检查是否需要验证码
            if '请输入验证码' in r.text or 'verifycode' in r.text or 'captcha-verify' in r.text:
                logging.warning(f"访问 {url} 触发验证码验证")
                return None
            return r.text
        except requests.exceptions.RequestException as e:
            logging.error(f"[get_page] 请求失败 (尝试 {attempt + 1}/{RETRY_TIMES}): {url} -> {e}")
            if attempt < RETRY_TIMES - 1:
                time.sleep(random.uniform(1, 3))
                continue
    return None

def fetch_and_save_regions_prices():
    """从主页面动态获取区域和价格信息，并保存到文件"""
    logging.info(f"正在从 {COMMON_BASE_URL} 获取区域和价格信息...")
    html = get_page(COMMON_BASE_URL)
    if not html:
        logging.critical(f"无法获取主页面 {COMMON_BASE_URL}，程序无法继续。")
        sys.exit(1)

    doc = pq(html)
    logging.info("主页面解析成功，开始提取区域和价格信息...")

    # --- 1. 获取区域信息 ---
    regions = []
    region_ul = doc('#__layout > div > section > section.filter > div.filter-wrap.filter-region > section > div > ul').eq(0)
    if not region_ul:
        logging.warning("未找到区域列表的UL标签（主选择器），尝试备用选择器...")
        region_ul = doc('ul').has('li.region-item').eq(0)

    if region_ul:
        logging.info(f"成功找到区域列表UL标签，共找到 {len(region_ul.find('li'))} 个li元素")
        for i, li in enumerate(region_ul.find('li')[1:], start=1):
            li_doc = pq(li)
            a_tag = li_doc.find('a').eq(0)
            if not a_tag: continue
            region_name = a_tag.text().strip()
            region_href = a_tag.attr('href')
            if not region_name or not region_href: continue
            path_match = re.search(r'/community/([^/]+)/', region_href)
            region_path = path_match.group(1) if path_match else None
            if region_path:
                regions.append({'name': region_name, 'path': region_path, 'href': region_href})
                logging.debug(f"提取区域：名称={region_name}，路径={region_path}")
        regions = [r for r in regions if r['name'] and r['path']]
        logging.info(f"成功提取 {len(regions)} 个有效区域")
    else:
        logging.error("未找到任何区域列表相关的UL标签")

    # --- 2. 获取价格信息 ---
    prices = []
    price_ul = doc('#__layout > div > section > section.filter > div:nth-child(2) > section > ul').eq(0)
    if not price_ul:
        logging.warning("未找到价格列表的UL标签（主选择器），尝试备用选择器...")
        price_ul = doc('ul').has('li.line-item').eq(0)

    if price_ul:
        logging.info(f"成功找到价格列表UL标签，共找到 {len(price_ul.find('li'))} 个li元素")
        for i, li in enumerate(price_ul.find('li')[1:8], start=1):
            li_doc = pq(li)
            a_tag = li_doc.find('a').eq(0)
            if not a_tag: continue
            price_href = a_tag.attr('href')
            if not price_href: continue
            price_match = re.search(r'/m(\d{4})/', price_href)
            if price_match:
                price_id = f"m{price_match.group(1)}"
                price_name = a_tag.text().strip()
                prices.append({'id': price_id, 'name': price_name, 'href': price_href})
                logging.debug(f"提取价格分段：ID={price_id}，名称={price_name}")
        prices = list({p['id']: p for p in prices}.values())
        price_ids = [p['id'] for p in prices]
        logging.info(f"成功提取 {len(price_ids)} 个有效价格分段：{price_ids}")
    else:
        logging.error("未找到任何价格列表相关的UL标签")

    if not regions or not price_ids:
        logging.critical("未能提取到有效的区域或价格信息，程序退出。")
        sys.exit(1)

    data = {
        'regions': regions,
        'price_ids': price_ids,
        'price_details': prices,
        'fetched_at': datetime.datetime.now().isoformat()
    }
    with open(REGIONS_PRICES_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info(f"成功获取并保存区域和价格信息到 {REGIONS_PRICES_FILE}")
    return data

def get_houses_url(html) -> List[str]:
    """从列表页HTML中提取小区详情页链接"""
    if not html: return []
    doc = pq(html)
    house_links = doc('#__layout > div > section > section.list-main > section > div.list-cell > a')

    urls = []
    for a in house_links.items():
        href = a.attr('href')
        if href and href.startswith('https://') and '/community/view/' in href:
            urls.append(href)

    if not urls:
        logging.debug("调试信息: 未找到任何小区链接。尝试查找 .list-cell 元素...")
        list_cell_elem = doc('.list-cell')
        if list_cell_elem:
            list_cell_html = list_cell_elem.html()
            logging.debug(f"调试信息: .list-cell 元素存在，其HTML内容为: {list_cell_html[:200] if list_cell_html else 'None'}")
        else:
            logging.debug("调试信息: 页面上未找到 .list-cell 元素。")

    logging.info(f"从当前页提取到 {len(urls)} 个小区链接。")
    return urls

def get_house_info(html) -> Optional[Dict]:
    if not html: return None
    try:
        doc = pq(html)
    except Exception as e:
        logging.error(f"[get_house_info] 解析错误: {e}")
        return None
    return {
        'title': safe_text(doc, '.community-title .title'),
        'type': safe_text(doc, '.info-list .column-2:nth-child(1) .value'),
        'price': safe_text(doc, '.house-price_compare .average'),  # 价格提取
        'time': safe_text(doc, '.info-list .column-2:nth-child(3) .value'),
        'owner': safe_text(doc, '.info-list .column-2:nth-child(4) .value'),
        'number': safe_text(doc, '.info-list .column-2:nth-child(5) .value'),
        'space': safe_text(doc, '.info-list .column-2:nth-child(6) .value'),
        'ratio': safe_text(doc, '.info-list .column-2:nth-child(7) .value'),
        'bulid': safe_text(doc, '.info-list .column-2:nth-child(9) .value'),
        'commercial': safe_text(doc, '.info-list .column-2:nth-child(10) .value'),
        'company': safe_text(doc, '.info-list .column-1:nth-child(17) .value'),
        'addr': safe_text(doc, '.community-title .sub-title'),
        'develop': safe_text(doc, '.info-list .column-1:nth-child(19) .value'),
        'scrape_time': datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S'),
    }

def extract_community_id_from_url(house_url) -> Optional[str]:
    m = re.search(r'/community/view/(\d+)', house_url)
    if m: return m.group(1)
    m = re.search(r'(\d{5,8})', house_url)
    return m.group(1) if m else None


def get_lat_lng_from_pano(base_url, community_id, house_url, region_name, price_id, page_idx, item_idx, progress) -> \
Tuple[Optional[float], Optional[float]]:
    """
    从全景接口获取经纬度，3次失败后触发手动验证
    """
    if not community_id: return (None, None)

    candidates = [
        f"{base_url}/esf-ajax/community/pc/pano?community_id={community_id}&comm_id={community_id}",
        f"{base_url}/esf-ajax/community/pc/pano?cid=20&community_id={community_id}&comm_id={community_id}",
    ]

    failed_api_url = None  # 记录失败的接口链接
    # 第一次尝试获取经纬度
    for url in candidates:
        for attempt in range(RETRY_TIMES):
            try:
                proxies = {'http': get_proxy(), 'https': get_proxy()} if get_proxy() else None
                r = session.get(url, timeout=10, proxies=proxies)
                if r.status_code == 200:
                    d = r.json().get('data')
                    if d:
                        lat = d.get('lat') or d.get('latitude')
                        lng = d.get('lng') or d.get('longitude')
                        if lat and lng: return (float(lat), float(lng))
            except Exception as e:
                logging.warning(f"[get_lat_lng] 获取失败 (尝试 {attempt + 1}/{RETRY_TIMES}): {url} -> {e}")
                failed_api_url = url  # 记录最后一次失败的接口链接
                time.sleep(random.uniform(0.5, 1.5))

    # 3次尝试失败，触发手动验证（使用失败的接口链接）
    verify_url = failed_api_url if failed_api_url else house_url  # 优先用接口链接，无则降级用详情页
    logging.warning(f"[get_lat_lng] 经纬度获取失败，触发手动验证（验证链接：{verify_url}）")
    user_choice = prompt_manual_intervention(
        verify_url,  # 传入失败的接口链接
        region_name, price_id, page_idx, item_idx,
        f"经纬度接口请求失败，需验证接口链接：{verify_url}"
    )

    # 根据用户选择处理
    if user_choice:
        # 用户选择继续，重新尝试获取经纬度
        for url in candidates:
            try:
                proxies = {'http': get_proxy(), 'https': get_proxy()} if get_proxy() else None
                r = session.get(url, timeout=10, proxies=proxies)
                if r.status_code == 200:
                    d = r.json().get('data')
                    if d:
                        lat = d.get('lat') or d.get('latitude')
                        lng = d.get('lng') or d.get('longitude')
                        if lat and lng:
                            logging.info(f"[get_lat_lng] 验证后获取经纬度成功: lat={lat}, lng={lng}")
                            return (float(lat), float(lng))
            except Exception as e:
                logging.warning(f"[get_lat_lng] 验证后重试失败: {url} -> {e}")
                time.sleep(random.uniform(0.5, 1.5))

    # 用户选择跳过或重试失败，返回None
    return (None, None)


def save_to_mongodb(house_info: Dict, batch: bool = True):
    global batch_cache
    if batch:
        batch_cache.append(house_info)
        if len(batch_cache) >= BATCH_INSERT_SIZE:
            try:
                # 提取缓存中所有url，查询已存在的记录
                cache_urls = [info['url'] for info in batch_cache]
                existing_urls = set(
                    doc['url'] for doc in collection.find({'url': {'$in': cache_urls}}, {'url': 1})
                )

                valid_cache = []
                for info in batch_cache:
                    # 跳过已存在的记录
                    if info['url'] in existing_urls:
                        logging.debug(f"跳过重复数据: {info['url']}")
                        continue
                    info.pop('_id', None)
                    valid_cache.append(info)

                if not valid_cache:
                    logging.info("批量缓存中无新数据，跳过插入")
                    batch_cache.clear()
                    return

                result = collection.insert_many(valid_cache, ordered=False)
                logging.info(f"批量插入 {len(result.inserted_ids)} 条新数据（过滤掉 {len(batch_cache) - len(valid_cache)} 条重复数据）")
                batch_cache.clear()
            except Exception as e:
                logging.error(f"批量插入失败: {e}")
                for info in batch_cache:
                    try:
                        info.pop('_id', None)
                        collection.update_one(
                            {'url': info['url']},
                            {'$set': info},
                            upsert=True
                        )
                        logging.debug(f"单条插入/更新成功: {info['url']}")
                    except Exception as single_e:
                        logging.error(f"单条插入/更新失败 ({info['url']}): {single_e}")
                batch_cache.clear()
    else:
        try:
            house_info.pop('_id', None)
            collection.update_one({'url': house_info['url']}, {'$set': house_info}, upsert=True)
            logging.debug(f"单条插入/更新成功: {house_info['url']}")
        except Exception as e:
            logging.error(f"单条插入/更新失败 ({house_info['url']}): {e}")


def save_checkpoint(region_name, price_id, page_idx, item_idx, next_url, reason=None, total_progress=None):
    data = {
        "region_name": region_name, "price_id": price_id, "page_idx": int(page_idx),
        "item_idx": int(item_idx), "next_url": next_url, "reason": reason,
        "timestamp": datetime.datetime.now().isoformat(), "total_progress": total_progress,
        "batch_cache_size": len(batch_cache)
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info(f"[checkpoint] 已保存: {region_name} > {price_id} > 第{page_idx}页 > 第{item_idx}个小区")

def load_checkpoint() -> Optional[Dict]:
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f: return json.load(f)
        except Exception as e: logging.error(f"读取checkpoint失败: {e}")
    return None

def prompt_manual_intervention(house_url, region_name, price_id, page_idx, item_idx, reason):
    logging.warning(f"\n[!]== 遇到问题，暂停爬取 ==[!]")
    logging.warning(f"区域: {region_name} > 价位: {price_id} > 页面: 第 {page_idx} 页 > 小区: 第 {item_idx} 个")
    logging.warning(f"URL: {house_url}")
    logging.warning(f"原因: {reason}")
    save_checkpoint(region_name, price_id, page_idx, item_idx, house_url, reason)
    try:
        webbrowser.open(house_url)
        print(f"\n浏览器已打开该链接，请手动完成验证（如输入验证码、登录），完成后返回控制台。")
    except Exception as e:
        logging.error(f"尝试打开浏览器失败: {e}")
        print(f"\n请手动访问链接完成验证: {house_url}")
    while True:
        cmd = input("处理完后请输入 'y' 继续，'q' 退出，'s' 跳过: ").strip().lower()
        if cmd == 'y':
            logging.info("继续爬取...")
            return True
        elif cmd == 'q':
            logging.info("退出并保留断点。")
            if batch_cache:
                try: collection.insert_many(batch_cache, ordered=False); logging.info(f"退出时保存了 {len(batch_cache)} 条缓存数据")
                except Exception as e: logging.error(f"退出时保存缓存数据失败: {e}")
            sys.exit(0)
        elif cmd == 's':
            logging.info("跳过当前链接，继续爬取...")
            return False


def extract_total_count(html, base_url) -> Optional[int]:
    """从基础链接HTML中提取小区总数"""
    if not html:
        logging.warning(f"extract_total_count: 传入的html为空 (基础链接: {base_url})")
        return None

    try:
        doc = pq(html)
        total_text = safe_text(doc,
                               '#__layout > div > section > section.list-main > section > div.sort-row > span.total-info')

        if not total_text:
            logging.warning(f"未找到 .total-info 元素或元素文本为空 (基础链接: {base_url})。尝试备用选择器...")
            total_text = safe_text(doc, '.sort-row .total-info') or safe_text(doc, '.result-count') or safe_text(doc,
                                                                                                                 '.count')

        if not total_text:
            # --- 检查是否存在“暂未找到相关小区”的提示 ---
            empty_text_selector = '#__layout > div > section > section.list-main > section > section > span.empty-text'
            empty_text = safe_text(doc, empty_text_selector)

            if empty_text and "暂未找到相关小区" in empty_text:
                logging.info(f"页面明确提示'暂未找到相关小区'，确认该价位板块无数据。 (链接: {base_url})")
                return 0  # 返回0，表示没有小区

            # --- 原有逻辑：触发安全验证检查 ---
            match = re.search(r'community/([^/]+)/([^/]+)', base_url)
            region_path, price_id = match.groups() if match else ("unknown_region", "unknown_price")

            # 调用辅助函数处理
            html = check_for_security_verification_and_retry(html, base_url, region_path, price_id)

            # 如果辅助函数返回了新的HTML（用户验证成功），则重新解析
            if html:
                doc = pq(html)
                total_text = safe_text(doc,
                                       '#__layout > div > section > section.list-main > section > div.sort-row > span.total-info')
                if not total_text:
                    total_text = safe_text(doc, '.sort-row .total-info') or safe_text(doc,
                                                                                      '.result-count') or safe_text(doc,
                                                                                                                    '.count')

        if not total_text:
            sort_row_elem = doc('.sort-row')
            if sort_row_elem:
                sort_row_html = sort_row_elem.html()
                logging.debug(
                    f"调试信息: .sort-row 元素存在，HTML内容: {sort_row_html[:200] if sort_row_html else 'None'} (基础链接: {base_url})")
            else:
                logging.debug(f"调试信息: 未找到 .sort-row 元素 (基础链接: {base_url})")
            return None

        logging.info(f"找到总数文本: '{total_text}'")

        match = re.search(r'共找到\s*(\d+)\s*个小区', total_text)
        if not match:
            match = re.search(r'找到\s*(\d+)\s*个结果', total_text) or re.search(r'(\d+)\s*个小区', total_text)

        return int(match.group(1)) if match else None
    except Exception as e:
        logging.error(f"extract_total_count 解析错误 (基础链接: {base_url}): {e}", exc_info=True)
        return None

def check_for_security_verification_and_retry(html, url, region_path, price_id) -> Optional[str]:
    """
    检查HTML中是否存在“安全验证”
    """
    # 确保调试目录存在
    os.makedirs(DEBUG_HTML_DIR, exist_ok=True)

    # 1. 保存HTML文件
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{DEBUG_HTML_DIR}/debug_{region_path}_{price_id}_{timestamp}.html"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html)
    logging.info(f"已将出错页面的HTML保存至: {filename}")

    # 2. 检查是否包含“安全验证”
    if '安全验证' in html:
        logging.warning("检测到页面包含 '安全验证' 字样。")

        # 3. 提取第二个https链接
        https_links = re.findall(r'https://[^\s"\']+', html)
        verification_url = None
        if len(https_links) >= 2:
            verification_url = https_links[1]
            logging.info(f"提取到HTTPS链接 (用于验证): {verification_url}")
        else:
            logging.error("未在页面中找到HTTPS链接，无法自动提取验证链接。")
            # 提示用户查看保存的HTML文件
            print(f"\n请手动打开{COMMON_BASE_URL}完成安全验证。")
            print("请点击原始链接完成安全验证:", url)

        # 4. 暂停并提示用户
        logging.warning(f"\n[!]== 触发安全验证，请手动处理 ==[!]")
        logging.warning(f"请点击或访问以下链接进行安全验证: {verification_url if verification_url else '请查看HTML文件'}")
        logging.warning(f"完成验证后，回到此控制台。")

        if verification_url:
            try:
                webbrowser.open(verification_url)
            except Exception as e:
                logging.error(f"尝试自动打开浏览器失败: {e}")

        while True:
            cmd = input("\n完成验证后，请输入 'y' 重试原始链接，'s' 跳过此链接，'q' 退出: ").strip().lower()
            if cmd == 'y':
                logging.info("用户已完成验证，正在重试原始链接...")
                # 5. 重试原始链接
                new_html = get_page(url)
                if new_html:
                    logging.info("重试成功，获取到新的页面内容。")
                    return new_html
                else:
                    logging.error("重试失败，链接仍然无法访问。")
                    return None
            elif cmd == 's':
                logging.info("用户选择跳过此链接。")
                return None
            elif cmd == 'q':
                logging.info("用户选择退出。")
                if batch_cache:
                    try: collection.insert_many(batch_cache, ordered=False); logging.info(f"退出时保存了 {len(batch_cache)} 条缓存数据")
                    except Exception as e: logging.error(f"退出时保存缓存数据失败: {e}")
                sys.exit(0)
    else:
        logging.info("页面中未发现 '安全验证' 字样，可能该价位没有小区，可选择打开链接检验。")

    return None

def calculate_progress(region_index: int, price_index: int, page_idx: int, total_pages: int) -> float:
    total_regions = len(CRAWL_TASKS)
    total_prices = len(COMMON_PRICE_IDS)
    if total_regions == 0 or total_prices == 0: return 0.0
    completed_regions = region_index
    completed_prices = price_index
    page_progress = (page_idx - 1) / total_pages if total_pages > 0 else 0
    total_steps = total_regions * total_prices
    current_step = completed_regions * total_prices + completed_prices + page_progress
    return round((current_step / total_steps) * 100, 2)

# --- 主爬取逻辑 ---
def crawl_price_segment(region_info: Dict, price_id: str, start_page=1, start_item=1, region_index=0, price_index=0) -> bool:
    region_name = region_info['name']
    region_path = region_info['path']

    # 构造不带分页的基础链接
    base_url = f"{COMMON_BASE_URL}/{region_path}/{price_id}"
    logging.info(f"\n{'='*60}")
    logging.info(f"开始爬取: {region_name} ({region_path}) - 价位板块: {price_id}")
    logging.info(f"基础链接 (无分页): {base_url}")
    logging.info(f"{'='*60}")

    session.headers.update({'Referer': base_url})

    # 先访问基础链接获取小区总数
    base_html = get_page(base_url)
    if not base_html:
        user_continue = prompt_manual_intervention(base_url, region_name, price_id, 1, 0, "获取基础链接时触发验证码")
        if not user_continue: return False
        base_html = get_page(base_url)
        if not base_html:
            logging.error(f"手动处理后仍无法获取基础链接 (链接: {base_url})")
            return False

    # 从基础链接HTML中提取总数
    total_count = extract_total_count(base_html, base_url)
    if total_count is None:
        logging.warning(f"无法获取 {region_name} - {price_id} 的小区总数 (基础链接: {base_url})，尝试直接解析基础链接的小区链接。")
        houses_urls = get_houses_url(base_html)
        if not houses_urls:
            logging.warning(f"基础链接未找到任何小区链接 (链接: {base_url})，跳过此价位。")
            return True
        total_pages = 1
        logging.info(f"按基础链接解析到的 {len(houses_urls)} 个小区开始处理)")
    else:
        total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        logging.info(f"找到 {total_count} 个小区，共 {total_pages} 页)")

    if start_page > total_pages:
        logging.warning(f"自定义起始页码 {start_page} 大于总页数 {total_pages}，跳过此价位板块。")
        return True

    crawled_count = 0

    for page_idx in range(start_page, total_pages + 1):
        if page_idx == 1:
            page_url = base_url  # 第一页用基础链接（无分页）
        else:
            page_url = f"{base_url}-p{page_idx}/#filtersort"  # 第二页及以后用 "-p{page_idx}" 格式

        progress = calculate_progress(region_index, price_index, page_idx, total_pages)
        logging.info(f"\n--- 正在抓取页面: {page_idx}/{total_pages} | 整体进度: {progress}% ---")
        logging.info(f"当前分页链接: {page_url}")

        # 第一页使用基础链接的HTML，避免重复请求
        page_html = base_html if page_idx == 1 else get_page(page_url)
        if not page_html:
            user_continue = prompt_manual_intervention(page_url, region_name, price_id, page_idx, 1, "获取分页链接时触发验证码")
            if not user_continue: return False
            page_html = get_page(page_url)
            if not page_html:
                logging.error(f"手动处理后仍无法获取分页链接 (链接: {page_url})，跳过此页。")
                continue

        houses_urls = get_houses_url(page_html)
        if not houses_urls:
            logging.warning(f"第 {page_idx} 页未找到小区链接 (链接: {page_url})，跳过此页。")
            continue

        current_start_item = start_item if page_idx == start_page else 1
        for item_idx, house_url in enumerate(houses_urls, start=1):
            if item_idx < current_start_item:
                logging.info(f"跳过已处理的小区: 第 {item_idx} 个 -> {house_url}")
                continue

            if collection.count_documents({'url': house_url}, limit=1) > 0:
                logging.info(f"小区 {house_url} 已存在于数据库，跳过爬取")
                continue

            logging.info(f"--- 正在处理第 {item_idx}/{len(houses_urls)} 个小区: {house_url}")
            save_checkpoint(region_name, price_id, page_idx, item_idx, house_url, "正常爬取中", progress)

            house_html = get_page(house_url)
            if not house_html:
                user_continue = prompt_manual_intervention(house_url, region_name, price_id, page_idx, item_idx, "获取详情页时触发验证码")
                if not user_continue:
                    save_checkpoint(region_name, price_id, page_idx, item_idx + 1, house_url, "跳过无法获取的详情页", progress)
                    continue

            house_info = get_house_info(house_html)
            if not house_info:
                logging.warning(f"详情页 {house_url} 解析失败，跳过此小区。")
                save_checkpoint(region_name, price_id, page_idx, item_idx + 1, house_url, "跳过无法解析的详情页", progress)
                continue

            community_id = extract_community_id_from_url(house_url)
            parsed_house_url = urlparse(house_url)
            base_domain = f"{parsed_house_url.scheme}://{parsed_house_url.netloc}"

            # 调用修改后的经纬度获取函数，传入必要参数
            lat, lng = get_lat_lng_from_pano(
                base_domain, community_id, house_url,
                region_name, price_id, page_idx, item_idx, progress
            )

            if not (lat and lng):
                logging.warning(f"无法获取 {house_url} 的经纬度")

            house_info.update({
                'url': house_url, 'community_id': community_id, 'lat': lat, 'lng': lng,
                'region_name': region_name, 'region_path': region_path, 'price_segment': price_id
            })

            save_to_mongodb(house_info)
            crawled_count += 1

            # --- 暂停逻辑 ---
            base_sleep = random.normalvariate(0.9, 0.2)
            base_sleep = max(0.6, min(1.2, base_sleep))
            logging.info(f"[暂停] 基础暂停 {base_sleep:.2f} 秒...")
            time.sleep(base_sleep)

            if crawled_count % 25 == 0:
                extra_sleep = 3
                logging.info(f"[暂停] 已爬取 {crawled_count} 个小区，额外暂停 {extra_sleep} 秒...")
                time.sleep(extra_sleep)
            elif crawled_count % 10 == 0:
                extra_sleep = 2
                logging.info(f"[暂停] 已爬取 {crawled_count} 个小区，额外暂停 {extra_sleep} 秒...")
                time.sleep(extra_sleep)
            elif crawled_count % 5 == 0:
                extra_sleep = 1
                logging.info(f"[暂停] 已爬取 {crawled_count} 个小区，额外暂停 {extra_sleep} 秒...")
                time.sleep(extra_sleep)

        start_item = 1

    if batch_cache:
        try:
            collection.insert_many(batch_cache, ordered=False)
            logging.info(f"当前价位爬取完成，保存了 {len(batch_cache)} 条缓存数据")
            batch_cache.clear()
        except Exception as e:
            logging.error(f"保存缓存数据失败: {e}")

    logging.info(f"\n{region_name} - {price_id} 爬取完成！共爬取 {crawled_count} 个小区。")
    return True

def main():
    global CRAWL_TASKS, COMMON_PRICE_IDS, ENABLE_CUSTOM_START

    # 动态加载区域和价格信息
    if os.path.exists(REGIONS_PRICES_FILE):
        try:
            with open(REGIONS_PRICES_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                CRAWL_TASKS = data['regions']
                COMMON_PRICE_IDS = data['price_ids']
                logging.info(f"从 {REGIONS_PRICES_FILE} 加载区域和价格信息。")
        except Exception as e:
            logging.error(f"加载 {REGIONS_PRICES_FILE} 失败: {e}，将尝试重新获取。")
            os.remove(REGIONS_PRICES_FILE)
            data = fetch_and_save_regions_prices()
            CRAWL_TASKS = data['regions']
            COMMON_PRICE_IDS = data['price_ids']
    else:
        data = fetch_and_save_regions_prices()
        CRAWL_TASKS = data['regions']
        COMMON_PRICE_IDS = data['price_ids']

    if not CRAWL_TASKS or not COMMON_PRICE_IDS:
        logging.critical("未能加载区域或价格信息，程序退出。")
        sys.exit(1)

    # 程序启动时强制验证码验证（使用第一个基础链接）
    logging.info("\n=== 程序启动强制验证码验证 ===")
    first_region = CRAWL_TASKS[0]
    first_price = COMMON_PRICE_IDS[0]
    verify_base_url = f"{COMMON_BASE_URL}/{first_region['path']}/{first_price}"
    logging.info(f"强制验证链接: {verify_base_url}")
    prompt_manual_intervention(verify_base_url, "启动强制验证", first_price, 1, 0, "程序启动前强制完成验证码验证，避免后续爬取中断")

    # 打印配置信息
    logging.info("\n--- 动态获取到的配置信息 ---")
    logging.info(f"区域列表 ({len(CRAWL_TASKS)} 个): {[r['name'] for r in CRAWL_TASKS]}")
    logging.info(f"价格分段 ({len(COMMON_PRICE_IDS)} 个): {COMMON_PRICE_IDS}")
    logging.info("---------------------------------")

    # 检查自定义起始点
    if ENABLE_CUSTOM_START:
        region_names = [r['name'] for r in CRAWL_TASKS]
        if CUSTOM_START_REGION_NAME not in region_names or CUSTOM_START_PRICE_ID not in COMMON_PRICE_IDS:
            logging.warning(f"自定义起始点 '{CUSTOM_START_REGION_NAME}' > '{CUSTOM_START_PRICE_ID}' 无效，将从头开始爬取。")
            ENABLE_CUSTOM_START = False

    # 断点续爬逻辑
    cp = load_checkpoint()
    resume_from_checkpoint = False
    if cp:
        response = input(f"\n检测到断点文件 (上次进度: {cp.get('total_progress', 0)}%)，是否从断点继续？(y/n): ").strip().lower()
        if response == 'y':
            resume_from_checkpoint = True
            logging.info(f"将从断点 {cp['region_name']} > {cp['price_id']} > 第{cp['page_idx']}页 > 第{cp['item_idx']}个小区 继续爬取。")
        else:
            if os.path.exists(CHECKPOINT_FILE): os.remove(CHECKPOINT_FILE)
            logging.info("已删除旧的断点文件。")

    start_crawling = False
    total_regions = len(CRAWL_TASKS)
    for i, region_info in enumerate(CRAWL_TASKS):
        region_name = region_info['name']

        if not start_crawling:
            if resume_from_checkpoint:
                if region_name == cp['region_name']: start_crawling = True
                else: logging.info(f"\n跳过已完成的区域: {region_name}")
            elif ENABLE_CUSTOM_START:
                if region_name == CUSTOM_START_REGION_NAME: start_crawling = True
                else: logging.info(f"\n跳过自定义起始点之前的区域: {region_name}")
            else:
                start_crawling = True

        if not start_crawling: continue

        logging.info(f"\n{'='*60}")
        logging.info(f"进入区域: {region_name} (剩余 {total_regions - i - 1} 个区域)")
        logging.info(f"{'='*60}")

        # 遍历当前区域的所有价格分段
        total_prices = len(COMMON_PRICE_IDS)
        for j, price_id in enumerate(COMMON_PRICE_IDS):
            if resume_from_checkpoint and region_name == cp['region_name']:
                if price_id == cp['price_id']:
                    success = crawl_price_segment(
                        region_info, price_id,
                        start_page=cp['page_idx'],
                        start_item=cp['item_idx'],
                        region_index=i,
                        price_index=j
                    )
                    resume_from_checkpoint = False
                else:
                    logging.info(f"\n跳过已完成的价位板块: {price_id}")
                    continue
            elif ENABLE_CUSTOM_START and region_name == CUSTOM_START_REGION_NAME:
                if price_id == CUSTOM_START_PRICE_ID:
                    success = crawl_price_segment(
                        region_info, price_id,
                        start_page=CUSTOM_START_PAGE,
                        region_index=i,
                        price_index=j
                    )
                    ENABLE_CUSTOM_START = False
                else:
                    logging.info(f"\n跳过自定义起始点之前的价位板块: {price_id}")
                    continue
            else:
                success = crawl_price_segment(region_info, price_id, region_index=i, price_index=j)

            if not success:
                logging.error(f"\n爬取在区域 {region_name} > 价位 {price_id} 处中断。")
                if batch_cache:
                    try: collection.insert_many(batch_cache, ordered=False); logging.info(f"中断时保存了 {len(batch_cache)} 条缓存数据")
                    except Exception as e: logging.error(f"中断时保存缓存数据失败: {e}")
                return

    # 完成所有任务
    if batch_cache:
        try:
            collection.insert_many(batch_cache, ordered=False)
            logging.info(f"所有任务完成，保存了 {len(batch_cache)} 条缓存数据")
            batch_cache.clear()
        except Exception as e: logging.error(f"保存最后一批数据失败: {e}")

    logging.info("\n" + "="*60)
    logging.info("所有区域和价位的爬取任务全部完成！")
    if os.path.exists(CHECKPOINT_FILE): os.remove(CHECKPOINT_FILE)
    total_count = collection.count_documents({})
    logging.info(f"数据库中共有 {total_count} 条小区数据")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.warning("\n程序被用户中断。")
        if batch_cache:
            try: collection.insert_many(batch_cache, ordered=False); logging.info(f"中断时保存了 {len(batch_cache)} 条缓存数据")
            except Exception as e: logging.error(f"中断时保存缓存数据失败: {e}")
        sys.exit(0)
    except Exception as e:
        logging.critical(f"程序发生严重错误: {e}", exc_info=True)
        if batch_cache:
            try: collection.insert_many(batch_cache, ordered=False); logging.info(f"错误时保存了 {len(batch_cache)} 条缓存数据")
            except Exception as save_e: logging.error(f"错误时保存缓存数据失败: {save_e}")
        sys.exit(1)
