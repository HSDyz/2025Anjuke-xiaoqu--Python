"""
Created on 2025/11/13
@Author: YZ
Modified:
    1. 加入经纬度提取。
    2. 加入获取页面范围逻辑不再需要手动设置范围。
    3. 加入全流程，可以直接设置完所有任务，不需要再更换链接。
    4. 新增自定义起始爬取开关和剩余任务显示功能。
    5. 强化断点记录，确保精准续爬。
"""
import requests
import time
import random
import re
from tqdm import tqdm
from pyquery import PyQuery as pq
from pymongo import MongoClient
import datetime
from urllib.parse import urlparse
import webbrowser
import json
import os
import sys

# --- 核心配置区 ---
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'Anjuke'
COLLECTION_NAME = 'xiaoqu'
CHECKPOINT_FILE = "crawl_checkpoint.json"
PAGE_SIZE = 25  # 每页小区数量，固定不改

# --- 公共配置 ---
COMMON_BASE_URL = "https://chengdu.anjuke.com/community"
COMMON_PRICE_IDS = ['m3094', 'm3095', 'm3096', 'm3097', 'm3098', 'm3099', 'm3100']  # 均价分区

# --- 爬取任务清单 ---
CRAWL_TASKS = ['yubei', 'nanan', 'wuhou', 'jinniu', 'chenghua']

# --- 自定义起始爬取配置 ---
# 启用自定义起始点（True/False）
ENABLE_CUSTOM_START = False
# 自定义起始区域 (必须是 CRAWL_TASKS 中的一个)
CUSTOM_START_REGION = 'nanan'
# 自定义起始价位 (必须是 COMMON_PRICE_IDS 中的一个)
CUSTOM_START_PRICE = 'm3096'
# 自定义起始页码
CUSTOM_START_PAGE = 2

# --- 初始化 ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    'Referer': 'https://chongqing.anjuke.com/community/nanana/m3094-p1/',
    #'Cookie': '按需，加入cookie防反扒'
})

# --- 工具函数 ---
def safe_text(doc, selector):
    elem = doc(selector)
    return elem.text().strip() if elem else None

def get_page(url):
    try:
        r = session.get(url, timeout=15)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[get_page] 请求失败: {url} -> {e}")
        return None

def get_houses_url(html):
    if not html:
        return []
    doc = pq(html)
    return [a.attr('href') for a in doc('.list-cell a').items()
            if a.attr('href') and a.attr('href').startswith('https://') and '/community/view/' in a.attr('href') and not a.attr('href').endswith('/jiedu/')]

def get_house_info(html):
    if not html: return None
    try: doc = pq(html)
    except Exception as e:
        print(f"[get_house_info] parse error: {e}")
        return None
    return {
        'title': safe_text(doc, '.community-title .title'),
        'type': safe_text(doc, '.info-list .column-2:nth-child(1) .value'),
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
        'scrape_time': datetime.datetime.now().strftime('%Y/%m/%d'),
    }

def extract_community_id_from_url(house_url):
    m = re.search(r'/community/view/(\d+)', house_url)
    return m.group(1) if m else (re.search(r'(\d{5,8})', house_url).group(1) if re.search(r'(\d{5,8})', house_url) else None)

def get_lat_lng_from_pano(base_url, community_id):
    if not community_id: return (None, None)
    candidates = [
        f"{base_url}/esf-ajax/community/pc/pano?community_id={community_id}&comm_id={community_id}",
        f"{base_url}/esf-ajax/community/pc/pano?cid=20&community_id={community_id}&comm_id={community_id}",
    ]
    for url in candidates:
        try:
            r = session.get(url, timeout=10)
            if r.status_code == 200:
                d = r.json().get('data')
                if d:
                    lat = d.get('lat') or d.get('latitude')
                    lng = d.get('lng') or d.get('longitude')
                    if lat and lng: return (lat, lng)
        except (ValueError, requests.RequestException): continue
    return (None, None)

# --- 断点续爬函数 ---
def save_checkpoint(region_path, price_id, page_idx, item_idx, next_url, reason=None):
    data = {
        "region_path": region_path,
        "price_id": price_id,
        "page_idx": int(page_idx),
        "item_idx": int(item_idx),
        "next_url": next_url,
        "reason": reason,
        "timestamp": datetime.datetime.now().isoformat()
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\n[checkpoint] 已保存: {region_path} > {price_id} > 第{page_idx}页 > 第{item_idx}个小区 > URL: {next_url}")

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"读取 checkpoint 失败: {e}")
    return None

def prompt_manual_intervention(house_url, region_path, price_id, page_idx, item_idx, reason):
    print(f"\n[!]== 遇到问题，暂停爬取 ==[!]")
    print(f"区域: {region_path} > 价位: {price_id} > 页面: 第 {page_idx} 页 > 小区: 第 {item_idx} 个")
    print(f"URL: {house_url}")
    print(f"原因: {reason}")
    save_checkpoint(region_path, price_id, page_idx, item_idx, house_url, reason)
    try:
        webbrowser.open(house_url)
        print("浏览器已打开该链接，请处理（如完成验证码）。")
    except Exception as e:
        print(f"尝试打开浏览器失败: {e}")
    while True:
        cmd = input("处理完后请输入 'y' 继续，'q' 退出: ").strip().lower()
        if cmd == 'y':
            print("继续爬取...")
            return True
        elif cmd == 'q':
            print("退出并保留断点。")
            sys.exit(0)

def extract_total_count(html):
    if not html: return None
    match = re.search(r'共找到\s*(\d+)\s*个小区', safe_text(pq(html), '.total-info'))
    return int(match.group(1)) if match else None

# --- 主爬取逻辑 ---
def crawl_price_segment(region_path, price_id, start_page=1, start_item=1):
    region_base_url = f"{COMMON_BASE_URL}/{region_path}"
    current_segment_base_url = f"{region_base_url}/{price_id}"

    print(f"\n{'='*60}")
    print(f"开始爬取: {region_path} - 价位板块: {price_id}")
    print(f"{'='*60}")

    session.headers.update({'Referer': f'{current_segment_base_url}/p2/'})

    # 获取总页数
    first_page_url = f"{current_segment_base_url}/p1/#filtersort"
    first_page_html = get_page(first_page_url)
    if not first_page_html or ('verifycode' in first_page_html or 'captcha-verify' in first_page_html):
        prompt_manual_intervention(first_page_url, region_path, price_id, 1, 0, "获取总页数时遇到验证码或页面请求失败")
        first_page_html = get_page(first_page_url)

    total_count = extract_total_count(first_page_html)
    if not total_count:
        print(f"警告: 在 {region_path} - {price_id} 中未找到任何小区，跳过此价位。")
        return True

    total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
    print(f"找到 {total_count} 个小区，共 {total_pages} 页。")

    # 检查自定义起始页是否超出范围
    if start_page > total_pages:
        print(f"警告: 自定义起始页码 {start_page} 大于总页数 {total_pages}，将跳过此价位板块。")
        return True

    # 遍历页面
    for page_idx in range(start_page, total_pages + 1):
        # 【新增】显示剩余页数
        remaining_pages = total_pages - page_idx
        print(f"\n--- 正在抓取页面: {page_idx}/{total_pages} (剩余 {remaining_pages} 页) ---")
        page_url = f"{current_segment_base_url}/p{page_idx}/#filtersort"

        page_html = get_page(page_url)
        if not page_html:
            prompt_manual_intervention(page_url, region_path, price_id, page_idx, 1, "页面请求失败")
            page_html = get_page(page_url)
            if not page_html: return False

        if 'verifycode' in page_html or 'captcha-verify' in page_html:
            prompt_manual_intervention(page_url, region_path, price_id, page_idx, 1, "列表页遇到验证码")
            continue

        houses_urls = get_houses_url(page_html)
        if not houses_urls:
            print(f"警告: 第 {page_idx} 页未找到小区链接，可能已被封禁或页面结构改变。")
            save_checkpoint(region_path, price_id, page_idx, 1, page_url, "列表页无小区链接")
            return False

        current_start_item = start_item if page_idx == start_page else 1
        for item_idx, house_url in enumerate(houses_urls, start=1):
            if item_idx < current_start_item:
                print(f"跳过已处理的小区: 第 {item_idx} 个 -> {house_url}")
                continue

            print(f"--- 正在处理第 {item_idx}/{len(houses_urls)} 个小区: {house_url}")

            save_checkpoint(region_path, price_id, page_idx, item_idx, house_url, "正常爬取中")

            house_html = get_page(house_url)
            if not house_html:
                prompt_manual_intervention(house_url, region_path, price_id, page_idx, item_idx, "详情页无法请求")
                return False

            house_info = get_house_info(house_html)
            if not house_info:
                prompt_manual_intervention(house_url, region_path, price_id, page_idx, item_idx, "详情页解析失败")
                return False

            community_id = extract_community_id_from_url(house_url)
            parsed_house_url = urlparse(house_url)
            lat, lng = get_lat_lng_from_pano(f"{parsed_house_url.scheme}://{parsed_house_url.netloc}", community_id)
            if not (lat and lng):
                prompt_manual_intervention(house_url, region_path, price_id, page_idx, item_idx, "无法获取经纬度")
                return False

            house_info.update({
                'url': house_url, 'community_id': community_id, 'lat': lat, 'lng': lng,
                'region_path': region_path, 'price_segment': price_id
            })
            try:
                collection.update_one({'url': house_url}, {'$set': house_info}, upsert=True)
                print(f"成功保存: {house_info.get('title')}")
            except Exception as e:
                print(f"保存到 MongoDB 失败: {e}")

            time.sleep(random.uniform(0.5, 1.2))

        start_item = 1

    print(f"\n{region_path} - {price_id} 爬取完成！")
    return True

def main():
    # 断点续爬优先级高于自定义起始
    cp = load_checkpoint()
    resume_from_checkpoint = False
    if cp and input("检测到断点文件，是否从断点继续？(y/n): ").strip().lower() == 'y':
        resume_from_checkpoint = True
        print(f"将从断点 {cp['region_path']} > {cp['price_id']} > 第{cp['page_idx']}页 继续爬取。")
    else:
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)

    # 处理自定义起始逻辑
    start_crawling = False
    # 如果不是从断点续爬，再检查是否启用了自定义起始
    if not resume_from_checkpoint and ENABLE_CUSTOM_START:
        if CUSTOM_START_REGION in CRAWL_TASKS and CUSTOM_START_PRICE in COMMON_PRICE_IDS and CUSTOM_START_PAGE > 0:
            start_crawling = True
            print(f"\n已启用自定义起始爬取:")
            print(f"  起始区域: {CUSTOM_START_REGION}")
            print(f"  起始价位: {CUSTOM_START_PRICE}")
            print(f"  起始页码: {CUSTOM_START_PAGE}")
        else:
            print("\n警告：自定义起始配置无效，请检查 CUSTOM_START_* 变量是否正确设置。程序将从头开始爬取。")
            ENABLE_CUSTOM_START = False # 禁用无效的自定义起始

    # 外层循环：遍历所有区域路径
    total_regions = len(CRAWL_TASKS)
    for i, region_path in enumerate(CRAWL_TASKS):
        # 【新增】显示剩余区域
        remaining_regions = total_regions - i - 1

        # 逻辑判断：是否开始爬取当前区域
        if not start_crawling:
            if resume_from_checkpoint:
                if region_path == cp['region_path']:
                    start_crawling = True # 找到了断点所在的区域，开始爬取
                else:
                    print(f"\n跳过已完成的区域: {region_path}")
            elif ENABLE_CUSTOM_START:
                if region_path == CUSTOM_START_REGION:
                    start_crawling = True # 找到了自定义起始区域，开始爬取
                else:
                    print(f"\n跳过自定义起始点之前的区域: {region_path}")
            else: # 既不是断点续爬也不是自定义起始，直接开始
                start_crawling = True

        if not start_crawling:
            continue

        print(f"\n{'='*60}")
        print(f"进入区域: {region_path} (剩余 {remaining_regions} 个区域)")
        print(f"{'='*60}")

        # 中层循环：遍历当前区域的所有价位
        total_prices = len(COMMON_PRICE_IDS)
        for j, price_id in enumerate(COMMON_PRICE_IDS):
            # 显示当前区域剩余价位
            remaining_prices_in_region = total_prices - j - 1

            # 逻辑判断：是否开始爬取当前价位
            if resume_from_checkpoint and region_path == cp['region_path']:
                if price_id == cp['price_id']:
                    # 在断点所在的区域，找到了断点所在的价位
                    success = crawl_price_segment(region_path, price_id, start_page=cp['page_idx'], start_item=cp['item_idx'])
                    resume_from_checkpoint = False # 爬取完这个价位后，后续不再是断点续爬状态
                else:
                    print(f"\n跳过已完成的价位板块: {price_id}")
                    continue
            elif ENABLE_CUSTOM_START and region_path == CUSTOM_START_REGION:
                if price_id == CUSTOM_START_PRICE:
                    # 在自定义起始区域，找到了自定义起始价位
                    success = crawl_price_segment(region_path, price_id, start_page=CUSTOM_START_PAGE)
                    ENABLE_CUSTOM_START = False # 爬取完这个价位后，后续不再是自定义起始状态
                else:
                    print(f"\n跳过自定义起始点之前的价位板块: {price_id}")
                    continue
            else:
                # 正常爬取当前价位
                print(f"\n当前区域剩余价位: {remaining_prices_in_region} 个")
                success = crawl_price_segment(region_path, price_id)

            if not success:
                print(f"\n爬取在区域 {region_path} > 价位 {price_id} 处中断。")
                return

    # 所有任务完成
    print("\n" + "="*60)
    print("所有区域和价位的爬取任务全部完成！")
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("已删除断点文件。")

if __name__ == '__main__':
    main()
