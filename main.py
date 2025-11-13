"""
Created on 2025/11/13
@Author: YZ
Added: auto-open browser + checkpoint/resume when encountering None data
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

client = MongoClient('mongodb://localhost:27017/')
db = client['Anjuke']
collection = db['xiaoqu']

CHECKPOINT_FILE = "crawl_checkpoint.json"

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Referer': 'https://chongqing.anjuke.com/community/nanana/m3094-p2/',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    # 你的 cookie（如需）放在这里
})

def safe_text(doc, selector):
    elem = doc(selector)
    return elem.text().strip() if elem else None

def get_page(url):
    try:
        r = session.get(url, timeout=12)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[get_page] 请求失败: {url} -> {e}")
        return None

def get_houses_url(html):
    if not html:
        return []
    doc = pq(html)
    target_div = doc('.list-cell')
    urls = []
    for a in target_div('a').items():
        url = a.attr('href')
        if url and url.startswith('https://') and '/community/view/' in url and not url.endswith('/jiedu/'):
            urls.append(url)
    return urls

def get_house_info(html):
    if not html:
        return None
    try:
        doc = pq(html)
    except Exception as e:
        print("[get_house_info] parse error:", e)
        return None

    info = {
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
    return info

def extract_community_id_from_url(house_url):
    # 从 URL 中提取id 
    m = re.search(r'/community/view/(\d+)', house_url)
    if m:
        return m.group(1)
    # 备用提取数字
    m2 = re.search(r'(\d{5,8})', house_url)
    return m2.group(1) if m2 else None

def get_lat_lng_from_pano(base_url, community_id):
    """
    尝试请求 pano 接口并解析 lat/lng。
    base_url 例如: "https://chongqing.anjuke.com"
    community_id 字符串，例如 "594711"
    返回 (lat, lng) 或 (None, None)
    """
    if not community_id:
        return None, None

    # 尝试的 URL 列表
    candidates = [
        f"{base_url}/esf-ajax/community/pc/pano?community_id={community_id}&comm_id={community_id}",
        f"{base_url}/esf-ajax/community/pc/pano?cid=20&community_id={community_id}&comm_id={community_id}",
    ]

    for url in candidates:
        try:
            r = session.get(url, timeout=10)
            if r.status_code != 200:
                continue
            data = r.json()
            # data -> { "data": { "lat": "29.xxxx", "lng": "106.xxxx", ... } }
            d = data.get('data') if isinstance(data, dict) else None
            if not d:
                continue
            lat = d.get('lat') or d.get('latitude') or None
            lng = d.get('lng') or d.get('longitude') or None
            # 有时返回字符串，要确保是数字或字符串格式
            if lat and lng:
                return lat, lng
        except ValueError:
            # r.json() 解析失败
            continue
        except Exception as e:
            # 请求或其它错误，记录并继续尝试下一个 candidate
            continue

    return None, None

def save_checkpoint(page_idx, item_idx, url, reason=None):
    data = {
        "page_idx": int(page_idx),       # 1-based
        "item_idx": int(item_idx),       # 1-based
        "url": url,
        "reason": reason,
        "time": datetime.datetime.now().isoformat()
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[checkpoint] 已保存到 {CHECKPOINT_FILE}: page {page_idx}, item {item_idx}, reason={reason}")

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print("读取 checkpoint 失败:", e)
    return None

def prompt_manual_intervention(house_url, page_idx, item_idx, reason):
    """
    打开浏览器并保存 checkpoint，等待用户在命令行输入 y 继续或 q 退出（并保存）
    """
    print(f"\n[!]== 遇到问题，暂停爬取 ==[!]")
    print(f"页面: 第 {page_idx} 页, 第 {item_idx} 个小区")
    print(f"URL: {house_url}")
    print(f"原因: {reason}")
    # 保存 checkpoint
    save_checkpoint(page_idx, item_idx, house_url, reason=reason)
    # 在浏览器中打开
    try:
        webbrowser.open(house_url)
        print("浏览器已打开该链接，请在浏览器中检查/处理（例如完成验证码）。")
    except Exception as e:
        print("尝试打开浏览器失败:", e)
    # 等待用户输入
    while True:
        cmd = input("处理完后请输入 'y' 继续，'q' 退出并保留断点: ").strip().lower()
        if cmd == 'y':
            print("继续爬取...")
            return True
        elif cmd == 'q':
            print("退出并保留断点。")
            sys.exit(0)
        else:
            print("无效输入，请输入 y 或 q。")

def main():
    urls_1 = ['https://chengdu.anjuke.com/community/yubei/m3094-p' + str(i) + '/#filtersort' for i in range(1, 30)]
    urls = urls_1
    url_count = 0

    # 检查是否存在断点
    cp = load_checkpoint()
    resume = False
    start_page = 1
    start_item = 1
    if cp:
        print("检测到断点文件:", CHECKPOINT_FILE)
        print("断点内容：", cp)
        ans = input("是否从断点继续？(y继续 / n重新开始): ").strip().lower()
        if ans == 'y':
            resume = True
            start_page = int(cp.get("page_idx", 1))
            start_item = int(cp.get("item_idx", 1))
            print(f"将从 page {start_page}, item {start_item} 恢复。")
        else:
            print("将从头开始爬取（会覆盖旧断点）。")

    for page_idx, url in enumerate(urls, start=1):
        # 如果 resume 模式并且当前页小于 start_page，就跳过
        if resume and page_idx < start_page:
            continue

        page_processed = False
        while not page_processed:
            print('正在抓取：', url, f"(page {page_idx})")
            html = get_page(url)
            if html is None:
                print("页面请求失败，稍后重新尝试...")
                time.sleep(3)
                continue

            # 验证码/登录拦截检测
            if 'https://callback.58.com/antibot/verifycode?' in html or 'https://www.anjuke.com/captcha-verify/' in html:
                print("遇到登录或验证码验证，暂停操作，问题链接：", url)
                input("请在浏览器中完成人工操作后，按回车键继续...")
                continue

            houses_urls = get_houses_url(html)
            invalid_info_count = 0

            # 如果是 resume 且是起始页，用 start_item 跳过前面的 items
            start_from_item = start_item if (resume and page_idx == start_page) else 1

            for i, house_url in enumerate(houses_urls, start=1):
                # 若 resume 且本页要跳过前面 items
                if i < start_from_item:
                    print(f"跳过已处理的小区: page {page_idx} item {i}")
                    continue

                # 详情页短重试（网络不稳时避免立刻进入人工模式）
                house_html = None
                for attempt in range(3):
                    house_html = get_page(house_url)
                    if house_html:
                        break
                    time.sleep(1 + attempt)  # 逐步加短等待

                if not house_html:
                    # 记录并打开浏览器，等待人工干预
                    prompt_manual_intervention(house_url, page_idx, i, reason="详情页无法请求（重试后失败）")
                    # 用户确认后尝试重新获取一次
                    house_html = get_page(house_url)
                    if not house_html:
                        # 如果仍失败，保存断点并退出
                        save_checkpoint(page_idx, i, house_url, reason="详情页人工处理后仍无法请求")
                        print("详情页仍无法获取，已保存断点并退出。")
                        sys.exit(1)

                house_info = get_house_info(house_html)

                # 如果解析失败（None），直接进入人工处理流程
                if not house_info:
                    prompt_manual_intervention(house_url, page_idx, i, reason="详情页解析返回 None")
                    # 用户处理后再次尝试解析
                    house_html = get_page(house_url)
                    house_info = get_house_info(house_html)
                    if not house_info:
                        save_checkpoint(page_idx, i, house_url, reason="解析后仍然为 None")
                        print("解析仍失败，已保存断点并退出。")
                        sys.exit(1)

                # 提取 community id（通常在 URL 里）
                community_id = extract_community_id_from_url(house_url)
                parsed = urlparse(house_url)
                base_url = f"{parsed.scheme}://{parsed.netloc}"

                # pano 短重试
                lat, lng = None, None
                for attempt in range(2):
                    lat, lng = get_lat_lng_from_pano(base_url, community_id)
                    if lat and lng:
                        break
                    time.sleep(0.5 + attempt)

                if not (lat and lng):
                    # pano 无经纬度 -> 人工干预
                    prompt_manual_intervention(house_url, page_idx, i, reason="pano 返回无经纬度（lat/lng）")
                    # 用户处理后再次请求 pano
                    lat, lng = get_lat_lng_from_pano(base_url, community_id)
                    if not (lat and lng):
                        save_checkpoint(page_idx, i, house_url, reason="pano 人工处理后仍无经纬度")
                        print("pano 仍无法获取经纬度，已保存断点并退出。")
                        sys.exit(1)

                # 一切正常则写入 DB
                house_info['url'] = house_url
                house_info['community_id'] = community_id
                house_info['lat'] = lat
                house_info['lng'] = lng

                key = {'url': house_url} if house_url else {'community_id': community_id}
                try:
                    collection.update_one(key, {'$set': house_info}, upsert=True)
                    print(f"保存: {house_info.get('title')}  lat={lat} lng={lng}")
                except Exception as e:
                    print("保存到 MongoDB 失败:", e)

                url_count += 1
                time.sleep(random.uniform(0.7, 1.4))

                if 1 <= url_count <= 2:
                    for _ in tqdm(range(10), desc="短暂停"):
                        time.sleep(3 / 10)
                elif 4 <= url_count <= 5:
                    for _ in tqdm(range(10), desc="中暂停"):
                        time.sleep(8 / 10)
                elif 10 <= url_count <= 18:
                    for _ in tqdm(range(10), desc="长暂停"):
                        time.sleep(20 / 10)

                if url_count > 0 and url_count % 10 == 0:
                    for _ in tqdm(range(10), desc="长暂停"):
                        time.sleep(2)

            # 本页处理完成后，将 resume 标记重置（只对起始页有效）
            resume = False
            start_item = 1
            page_processed = True

if __name__ == '__main__':
    main()
