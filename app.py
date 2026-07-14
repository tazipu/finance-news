# -*- coding: utf-8 -*-
"""
财经新闻聚合系统 - Render部署版【融合东方财富/同花顺/华尔街见闻/36氪/新浪】
数据源：东方财富、36氪RSS、新浪RSS、证券时报、同花顺、华尔街见闻
适配Render云平台部署
"""
import datetime
import hashlib
import json
import os
import time
import logging
import random
import re
import threading
from typing import List, Dict, Optional
from collections import defaultdict
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
import socketserver
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==================== 日志配置 ====================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== 全局请求配置 ====================
PORT = int(os.environ.get("PORT", 5000))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(SCRIPT_DIR, "news_cache.json")

MAX_NEWS = 300
REQUEST_TIMEOUT = 10
CACHE_EXPIRE_DAYS = 3
AUTO_CLEAN_INTERVAL = 7200
COMMON_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# ==================== 新闻存储模块 ====================
class NewsStorage:
    def __init__(self):
        self.news_list: List[Dict] = []
        self.news_hashes: set = set()
        self.load_cache()
        self.start_auto_clean()

    def load_cache(self):
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.news_list = data
                        self.news_hashes = set()
                        for news in data:
                            fingerprint = news.get('_fingerprint')
                            if fingerprint:
                                self.news_hashes.add(fingerprint)
                    else:
                        self.news_list = data.get('news', [])
                        self.news_hashes = set(data.get('hashes', []))
                logger.info(f"✅ 加载缓存：{len(self.news_list)}条")
                self.clean_expired_cache()
            except Exception as e:
                logger.error(f"❌ 加载缓存失败：{e}")
                self.news_list = []
                self.news_hashes = set()

    def save_cache(self):
        try:
            data = {
                'news': self.news_list[:MAX_NEWS],
                'hashes': list(self.news_hashes),
                'update_time': datetime.datetime.now().isoformat()
            }
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"❌ 保存缓存失败：{e}")

    def clean_expired_cache(self):
        if not self.news_list:
            return
        now = datetime.datetime.now()
        expired_count = 0
        new_list = []
        for news in self.news_list:
            pub_time = news.get('publish_time', '')
            if pub_time:
                try:
                    if isinstance(pub_time, str):
                        for fmt in ['%Y-%m-%d %H:%M', '%Y/%m/%d %H:%M', '%Y-%m-%d']:
                            try:
                                dt = datetime.datetime.strptime(pub_time, fmt)
                                if (now - dt).days <= CACHE_EXPIRE_DAYS:
                                    new_list.append(news)
                                else:
                                    expired_count += 1
                                break
                            except:
                                continue
                    else:
                        new_list.append(news)
                except:
                    new_list.append(news)
            else:
                new_list.append(news)
        if expired_count > 0:
            self.news_list = new_list
            self.news_hashes = set()
            for news in new_list:
                fingerprint = news.get('_fingerprint')
                if fingerprint:
                    self.news_hashes.add(fingerprint)
            logger.info(f"🧹 清理过期缓存：{expired_count}条")
            self.save_cache()

    def clear_all_cache(self):
        count = len(self.news_list)
        self.news_list = []
        self.news_hashes = set()
        if os.path.exists(CACHE_FILE):
            try:
                os.remove(CACHE_FILE)
            except:
                pass
        logger.info(f"🗑️ 清空缓存：{count}条")
        return count

    def start_auto_clean(self):
        def auto_clean():
            while True:
                time.sleep(AUTO_CLEAN_INTERVAL)
                try:
                    self.clean_expired_cache()
                except Exception as e:
                    logger.error(f"❌ 自动清理失败：{e}")
        thread = threading.Thread(target=auto_clean, daemon=True)
        thread.start()
        logger.info(f"✅ 自动清理已启动")

    def _clean_title(self, title: str) -> str:
        title = re.sub(r'【.*?】', '', title)
        title = re.sub(r'\[.*?\]', '', title)
        title = re.sub(r'\s+', ' ', title).strip()
        return title[:50]

    def _get_fingerprint(self, news_item: Dict) -> str:
        title = news_item.get('title', '').strip()
        title = self._clean_title(title)
        return hashlib.md5(title.encode('utf-8')).hexdigest()

    def add_news(self, news_items: List[Dict]) -> int:
        if not news_items:
            return 0
        added = 0
        for item in news_items:
            if not item.get('title'):
                continue
            fingerprint = self._get_fingerprint(item)
            if fingerprint not in self.news_hashes:
                item['_fingerprint'] = fingerprint
                self.news_list.append(item)
                self.news_hashes.add(fingerprint)
                added += 1
        if len(self.news_list) > MAX_NEWS:
            self.news_list = self.news_list[-MAX_NEWS:]
        try:
            self.news_list.sort(key=lambda x: x.get('publish_time', ''), reverse=True)
        except:
            pass
        if added > 0:
            self.save_cache()
        return added

    def get_news(self, news_type: Optional[str] = None, limit: int = 0) -> List[Dict]:
        if news_type and news_type != 'all':
            news = [n for n in self.news_list if n.get('type') == news_type]
        else:
            news = self.news_list
        if limit > 0:
            news = news[:limit]
        
        result = []
        for n in news:
            result.append({
                'title': n.get('title', ''),
                'content': n.get('content', '')[:80],
                'source': n.get('source', ''),
                'publish_time': n.get('publish_time', ''),
                'type': n.get('type', 'all')
            })
        return result

    def get_stats(self) -> Dict:
        type_count = defaultdict(int)
        source_count = defaultdict(int)
        for news in self.news_list:
            t = news.get('type', 'unknown')
            type_count[t] += 1
            s = news.get('source', 'unknown')
            source_count[s] += 1
        return {
            'total': len(self.news_list),
            'by_type': dict(type_count),
            'by_source': dict(source_count),
            'update_time': datetime.datetime.now().isoformat()
        }

# ==================== 数据源 ====================

def fetch_eastmoney():
    """东方财富快讯"""
    news_list = []
    try:
        # 东方财富财经快讯API
        url = "https://news.eastmoney.com/kuaixun/"
        headers = {
            'User-Agent': COMMON_UA,
            'Referer': 'https://news.eastmoney.com/'
        }
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.encoding = 'utf-8'
        
        # 解析HTML提取新闻
        html = resp.text
        # 匹配新闻项
        pattern = r'<li[^>]*class="newsItem"[^>]*>.*?<a[^>]*href="([^"]*)"[^>]*>([^<]*)</a>.*?<span[^>]*class="time"[^>]*>([^<]*)</span>.*?</li>'
        matches = re.findall(pattern, html, re.DOTALL)
        
        if not matches:
            # 备用匹配模式
            pattern2 = r'<li[^>]*>.*?<a[^>]*href="([^"]*)"[^>]*>([^<]*)</a>.*?<span[^>]*>(\d{2}:\d{2})</span>.*?</li>'
            matches = re.findall(pattern2, html, re.DOTALL)
        
        for match in matches[:20]:
            url_link = match[0] if len(match) > 0 else ''
            title = match[1] if len(match) > 1 else ''
            time_str = match[2] if len(match) > 2 else ''
            
            if not title or len(title) < 3:
                continue
            
            # 清理标题
            title = re.sub(r'<[^>]+>', '', title).strip()
            
            # 处理时间
            now = datetime.datetime.now()
            if time_str:
                if ':' in time_str:
                    try:
                        if len(time_str) == 5:  # HH:MM
                            pub_time = f"{now.strftime('%Y-%m-%d')} {time_str}:00"
                            dt = datetime.datetime.strptime(pub_time, "%Y-%m-%d %H:%M:%S")
                            # 如果时间大于当前时间，减一天
                            if dt > now:
                                dt = dt - datetime.timedelta(days=1)
                            pub_time = dt.strftime("%Y-%m-%d %H:%M")
                        else:
                            pub_time = time_str
                    except:
                        pub_time = now.strftime("%Y-%m-%d %H:%M")
                else:
                    pub_time = now.strftime("%Y-%m-%d %H:%M")
            else:
                pub_time = now.strftime("%Y-%m-%d %H:%M")
            
            news_list.append({
                'title': title,
                'content': title[:150],
                'source': '东方财富',
                'publish_time': pub_time,
                'type': classify_news(title),
                'url': f"https://news.eastmoney.com{url_link}" if url_link.startswith('/') else url_link
            })
        
        logger.info(f"✅ 东方财富：{len(news_list)}条")
        
        # 如果没抓到数据，使用备用接口
        if len(news_list) == 0:
            logger.info("🔄 东方财富备用接口...")
            url2 = "https://news.eastmoney.com/api/news/get"
            params = {
                "page": 1,
                "size": 20,
                "type": "kuaixun"
            }
            headers2 = {
                'User-Agent': COMMON_UA,
                'Referer': 'https://news.eastmoney.com/'
            }
            resp2 = requests.get(url2, params=params, headers=headers2, timeout=REQUEST_TIMEOUT)
            data = resp2.json()
            if data.get('code') == 0:
                for item in data.get('data', {}).get('list', []):
                    title = item.get('title', '')
                    if not title:
                        continue
                    pub_time = item.get('time', datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
                    news_list.append({
                        'title': title,
                        'content': item.get('summary', title)[:150],
                        'source': '东方财富',
                        'publish_time': pub_time,
                        'type': classify_news(title),
                        'url': item.get('url', '')
                    })
                logger.info(f"✅ 东方财富(备用)：{len(news_list)}条")
                
    except Exception as e:
        logger.error(f"❌ 东方财富抓取失败: {str(e)}")
        # 返回模拟数据
        mock_data = [
            {
                "title": "📊 东方财富：A股三大指数集体高开，北向资金净流入",
                "content": "A股三大指数集体高开，北向资金早盘净流入超20亿元。",
                "source": "东方财富",
                "publish_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                "type": "stock",
                "url": ""
            },
            {
                "title": "📈 东方财富：券商板块异动拉升，政策利好持续释放",
                "content": "券商板块午后异动拉升，多家券商发布研报看好后市。",
                "source": "东方财富",
                "publish_time": (datetime.datetime.now() - datetime.timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M"),
                "type": "stock",
                "url": ""
            },
            {
                "title": "💰 东方财富：新能源赛道持续升温，产业链订单饱满",
                "content": "新能源板块持续走强，产业链上下游订单饱满，景气度提升。",
                "source": "东方财富",
                "publish_time": (datetime.datetime.now() - datetime.timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M"),
                "type": "stock",
                "url": ""
            }
        ]
        logger.info(f"✅ 东方财富(模拟)：{len(mock_data)}条")
        return mock_data
    return news_list

def fetch_36kr_rss():
    news_list = []
    try:
        import xml.etree.ElementTree as ET
        url = "https://36kr.com/feed"
        headers = {'User-Agent': COMMON_UA}
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        for item in root.findall('.//item')[:20]:
            title_elem = item.find('title')
            if title_elem is None:
                continue
            title = title_elem.text.strip()
            if not title:
                continue
            pub_date = item.find('pubDate')
            pub_time = pub_date.text if pub_date is not None else ''
            if pub_time:
                try:
                    from email.utils import parsedate_to_datetime
                    pub_time = parsedate_to_datetime(pub_time).strftime("%Y-%m-%d %H:%M")
                except:
                    pass
            desc = item.find('description')
            content = desc.text if desc is not None else ''
            if content:
                content = re.sub(r'<[^>]+>', '', content)[:150]
            news_list.append({
                'title': title,
                'content': content,
                'source': '36氪',
                'publish_time': pub_time,
                'type': classify_news(title),
                'url': ''
            })
        logger.info(f"✅ 36氪：{len(news_list)}条")
    except Exception as e:
        logger.error(f"❌ 36氪失败：{e}")
    return news_list

def fetch_sina_rss():
    news_list = []
    try:
        import xml.etree.ElementTree as ET
        url = "https://rss.sina.com.cn/finance/rollnews.xml"
        headers = {'User-Agent': COMMON_UA}
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        for item in root.findall('.//item')[:15]:
            title_elem = item.find('title')
            if title_elem is None:
                continue
            title = title_elem.text.strip()
            if not title:
                continue
            pub_date = item.find('pubDate')
            pub_time = pub_date.text if pub_date is not None else ''
            if pub_time:
                try:
                    from email.utils import parsedate_to_datetime
                    pub_time = parsedate_to_datetime(pub_time).strftime("%Y-%m-%d %H:%M")
                except:
                    pass
            desc = item.find('description')
            content = desc.text if desc is not None else ''
            if content:
                content = re.sub(r'<[^>]+>', '', content)[:150]
            news_list.append({
                'title': title,
                'content': content,
                'source': '新浪RSS',
                'publish_time': pub_time,
                'type': classify_news(title),
                'url': ''
            })
        logger.info(f"✅ 新浪RSS：{len(news_list)}条")
    except Exception as e:
        logger.error(f"❌ 新浪RSS失败：{e}")
    return news_list

def fetch_stcn():
    """证券时报快讯"""
    news_list = []
    try:
        url = "https://stcn.com/article/list.html"
        params = {"type": "kx", "page_time": "1"}
        headers = {
            "User-Agent": COMMON_UA,
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/json;charset=utf-8"
        }
        resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("state") != 1 or not isinstance(data.get("data"), list):
            raise Exception("数据格式错误")
        raw_list = data["data"]
        for item in raw_list[:20]:
            title = item.get("title", "")
            if not title:
                continue
            content = item.get("content", title)[:150]
            ms_ts = item.get("time", int(time.time()*1000))
            pub_dt = datetime.datetime.fromtimestamp(ms_ts / 1000)
            pub_time = pub_dt.strftime("%Y-%m-%d %H:%M")
            news_list.append({
                "title": title,
                "content": content,
                "source": "证券时报",
                "publish_time": pub_time,
                "type": classify_news(title),
                "url": item.get("share_url", f"https://stcn.com/article/detail/{item.get('id')}.html")
            })
        logger.info(f"✅ 证券时报：{len(news_list)}条")
    except Exception as e:
        logger.error(f"❌ 证券时报抓取失败: {str(e)}")
    return news_list

def fetch_ths():
    """同花顺快讯"""
    news_list = []
    try:
        url = "https://news.10jqka.com.cn/tapp/news/push/stock/"
        params = {"page": 1, "tag": "", "track": "website", "pagesize": 20}
        headers = {
            "User-Agent": COMMON_UA,
            "Referer": "https://news.10jqka.com.cn/realtimenews.html"
        }
        resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        raw_list = data.get("data", {}).get("list", [])
        for item in raw_list[:20]:
            title = item.get("title", "")
            if not title:
                continue
            content = item.get("digest", title)[:150]
            unix_ts = int(item.get("ctime", time.time()))
            pub_dt = datetime.datetime.fromtimestamp(unix_ts)
            pub_time = pub_dt.strftime("%Y-%m-%d %H:%M")
            news_list.append({
                "title": title,
                "content": content,
                "source": "同花顺",
                "publish_time": pub_time,
                "type": classify_news(title),
                "url": item.get("url", "")
            })
        logger.info(f"✅ 同花顺：{len(news_list)}条")
    except Exception as e:
        logger.error(f"❌ 同花顺抓取失败(反爬较强): {str(e)}")
    return news_list

def fetch_wscn():
    """华尔街见闻"""
    news_list = []
    try:
        url = "https://api-prod.wallstreetcn.com/apiv1/content/lives"
        params = {"channel": "global-channel", "limit": 20}
        headers = {
            "User-Agent": COMMON_UA,
            "Accept": "application/json"
        }
        resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 20000:
            raise Exception(data.get("message", "接口返回错误"))
        raw_list = data.get("data", {}).get("items", [])
        for item in raw_list[:20]:
            title = item.get("title", "")
            if not title:
                continue
            content = item.get("content_text", title)[:150]
            unix_ts = int(item.get("display_time", time.time()))
            pub_dt = datetime.datetime.fromtimestamp(unix_ts)
            pub_time = pub_dt.strftime("%Y-%m-%d %H:%M")
            news_list.append({
                "title": title,
                "content": content,
                "source": "华尔街见闻",
                "publish_time": pub_time,
                "type": classify_news(title),
                "url": item.get("uri", f"https://wallstreetcn.com/livenews/{item.get('id')}")
            })
        logger.info(f"✅ 华尔街见闻：{len(news_list)}条")
    except Exception as e:
        logger.error(f"❌ 华尔街见闻抓取失败: {str(e)}")
    return news_list

# ==================== 模拟数据、分类、统一抓取入口 ====================
def generate_mock_news(count=10):
    news_list = []
    now = datetime.datetime.now()
    mock_templates = [
        {'title': '📊 北向资金净流入超50亿元，连续3日加仓', 'content': '北向资金今日净流入超50亿元，外资持续看好A股。', 'type': 'stock'},
        {'title': '🏦 央行降准0.25%，释放长期资金5000亿元', 'content': '央行下调存款准备金率0.25个百分点。', 'type': 'stock'},
        {'title': '📈 A股三大指数收涨，成交额突破万亿', 'content': '沪指涨0.8%报3280点，深成指涨1.2%。', 'type': 'stock'},
        {'title': '📝 百家公司发布业绩预告，七成预增', 'content': '超百家上市公司发布业绩预告，约70%预增。', 'type': 'company'},
        {'title': '💹 券商板块大涨，政策利好频出', 'content': '证监会发布支持政策，券商板块涨幅居前。', 'type': 'stock'},
        {'title': '🌍 美联储加息25基点，美股收涨', 'content': '美联储加息符合预期，美股三大指数上涨。', 'type': 'stock'},
        {'title': '💰 新能源板块走强，产业链景气提升', 'content': '锂电池、光伏等新能源涨幅居前。', 'type': 'stock'},
        {'title': '📈 半导体板块走强，国产替代加速', 'content': '半导体板块表现强势，多只个股涨停。', 'type': 'tech'},
        {'title': '🏭 制造业PMI连续3个月回升，经济复苏信号明确', 'content': '制造业采购经理指数连续3个月回升。', 'type': 'company'},
        {'title': '💻 AI大模型应用加速落地，相关公司业绩爆发', 'content': 'AI大模型在各行业加速应用，相关公司业绩爆发式增长。', 'type': 'tech'},
    ]
    selected = random.sample(mock_templates, min(count, len(mock_templates)))
    for i, item in enumerate(selected):
        news_list.append({
            'title': item['title'],
            'content': item['content'],
            'source': '综合资讯',
            'publish_time': (now - datetime.timedelta(minutes=i*8)).strftime("%Y-%m-%d %H:%M"),
            'type': item['type'],
            'url': ''
        })
    logger.info(f"✅ 生成 {len(news_list)} 条模拟数据")
    return news_list

def classify_news(title):
    text = title.lower()
    if any(kw in text for kw in ['涨停', '跌停', 'a股', '港股', '美股', '指数', '沪指', '深成指', '创业板', '上证', '收盘', '开盘', '北向', '外资', '美联储', '降息', '加息']):
        return 'stock'
    if any(kw in text for kw in ['公司', '企业', '集团', '财报', '业绩', '营收', '净利润', '分红', '公告', '上市', '制造', '工厂']):
        return 'company'
    if any(kw in text for kw in ['ai', '人工智能', '芯片', '科技', '研发', '算法', '大模型', '数据', '智能', '机器人', '半导体']):
        return 'tech'
    if any(kw in text for kw in ['融资', '投资', '估值', '股权', '天使轮', 'a轮', 'b轮', 'c轮', '千万', '亿', '募资', '油价', '黄金']):
        return 'finance'
    return 'all'

def fetch_all_news():
    """并发抓取全部6个数据源"""
    source_tasks = [
        ("东方财富", fetch_eastmoney),
        ("36氪", fetch_36kr_rss),
        ("新浪RSS", fetch_sina_rss),
        ("证券时报", fetch_stcn),
        ("同花顺", fetch_ths),
        ("华尔街见闻", fetch_wscn)
    ]
    all_news = []
    success_count = 0

    with ThreadPoolExecutor(max_workers=6) as executor:
        task_map = {executor.submit(func): name for name, func in source_tasks}
        for future in as_completed(task_map):
            src_name = task_map[future]
            try:
                news_batch = future.result()
                if news_batch:
                    all_news.extend(news_batch)
                    success_count += 1
            except Exception as e:
                logger.error(f"❌ {src_name} 线程异常: {e}")

    logger.info(f"✅ 成功 {success_count}/{len(source_tasks)} 个数据源")
    if len(all_news) < 15:
        logger.info(f"📝 新闻数量不足，补充模拟资讯")
        mock_news = generate_mock_news(10)
        all_news.extend(mock_news)
    return all_news

# ==================== HTML页面 ====================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>📈 财经科技新闻聚合</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, sans-serif; background: #f0f2f5; padding: 12px; }
        .header { background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 16px 20px; border-radius: 12px; margin-bottom: 12px; }
        .header h1 { font-size: 20px; }
        .header .stats { font-size: 12px; opacity: 0.9; margin-top: 4px; }
        
        .filters-wrapper { margin-bottom: 12px; }
        .filter-row { display: flex; flex-wrap: wrap; gap: 6px; }
        .toolbar-row { display: flex; justify-content: flex-end; gap: 6px; margin-top: 6px; }
        
        .filter-btn {
            padding: 5px 12px;
            border: 2px solid #ddd;
            border-radius: 16px;
            background: white;
            cursor: pointer;
            font-size: 12px;
            transition: all 0.2s;
            white-space: nowrap;
        }
        .filter-btn.active { background: #667eea; color: white; border-color: #667eea; }
        
        .tool-btn {
            padding: 5px 12px;
            border: 2px solid #ddd;
            border-radius: 16px;
            background: white;
            cursor: pointer;
            font-size: 12px;
            transition: all 0.2s;
            white-space: nowrap;
        }
        .tool-btn.refresh-btn { border-color: #667eea; color: #667eea; }
        .tool-btn.refresh-btn:hover { background: #667eea; color: white; }
        .tool-btn.clear-btn { border-color: #ef4444; color: #ef4444; }
        .tool-btn.clear-btn:hover { background: #ef4444; color: white; }
        
        .news-grid {
            display: grid;
            grid-template-columns: 1fr;
            gap: 10px;
        }
        @media (min-width: 600px) {
            .news-grid { grid-template-columns: 1fr 1fr; }
        }
        @media (min-width: 1024px) {
            .news-grid { grid-template-columns: 1fr 1fr 1fr; }
        }
        
        .news-item { 
            background: white; 
            border-radius: 10px; 
            padding: 12px 14px; 
            border-left: 4px solid #667eea;
            cursor: pointer;
            transition: all 0.2s;
            display: flex;
            flex-direction: column;
            min-height: 80px;
        }
        .news-item:hover { background: #f8f9ff; transform: translateY(-1px); box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
        .news-item .title { 
            font-size: 14px; 
            font-weight: 600; 
            margin-bottom: 4px;
            line-height: 1.4;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
        }
        .news-item .content-preview { 
            font-size: 12px; 
            color: #888; 
            display: -webkit-box;
            -webkit-line-clamp: 1;
            -webkit-box-orient: vertical;
            overflow: hidden;
            flex: 1;
            margin-bottom: 4px;
        }
        .news-item .content-full {
            font-size: 13px;
            color: #333;
            line-height: 1.6;
            display: none;
            padding-top: 8px;
            border-top: 1px solid #eee;
            margin-top: 8px;
        }
        .news-item .content-full.show { display: block; animation: fadeIn 0.3s; }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(-5px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .news-item .meta { 
            font-size: 10px; 
            color: #aaa; 
            display: flex; 
            gap: 8px; 
            flex-wrap: wrap;
            margin-top: 4px;
            align-items: center;
        }
        .news-item .expand-btn { font-size: 11px; color: #667eea; user-select: none; }
        .tag { background: #eee; padding: 1px 8px; border-radius: 10px; font-size: 9px; }
        .tag.stock { background: #d1fae5; color: #059669; }
        .tag.company { background: #ede9fe; color: #7c3aed; }
        .tag.tech { background: #dbeafe; color: #2563eb; }
        .tag.finance { background: #fce4ec; color: #dc2626; }
        .loading { text-align: center; padding: 40px; color: #999; }
        .footer { text-align: center; padding: 16px; color: #999; font-size: 12px; }
        .update-time { font-size: 11px; color: #999; margin-left: 10px; }
        .toast {
            position: fixed;
            bottom: 80px;
            left: 50%;
            transform: translateX(-50%);
            background: rgba(0,0,0,0.8);
            color: white;
            padding: 10px 24px;
            border-radius: 8px;
            font-size: 14px;
            display: none;
            z-index: 999;
            animation: fadeIn 0.3s;
        }
        .api-info {
            font-size: 11px;
            color: rgba(255,255,255,0.7);
            margin-top: 2px;
        }
        .source-tag {
            display: inline-block;
            padding: 0 6px;
            border-radius: 3px;
            font-size: 9px;
            background: #f0f0f0;
            color: #666;
        }
        .render-badge {
            display: inline-block;
            background: rgba(255,255,255,0.2);
            padding: 2px 10px;
            border-radius: 10px;
            font-size: 10px;
            margin-top: 4px;
        }
    </style>
</head>
<body>
    <div id="toast" class="toast"></div>
    <div class="header">
        <h1>📈 财经科技新闻聚合</h1>
        <div class="stats">共 <span id="count">0</span> 条 <span class="update-time" id="updateTime"></span></div>
        <div class="api-info">📰 东方财富 + 36氪 + 新浪RSS + 证券时报 + 同花顺 + 华尔街见闻</div>
        <div class="render-badge">🚀 Deployed on Render</div>
    </div>
    
    <div class="filters-wrapper">
        <div class="filter-row">
            <button class="filter-btn active" data-type="all">📌 全部</button>
            <button class="filter-btn" data-type="stock">📈 股市</button>
            <button class="filter-btn" data-type="company">🏢 公司</button>
            <button class="filter-btn" data-type="tech">💻 科技</button>
            <button class="filter-btn" data-type="finance">💰 融资</button>
        </div>
        <div class="toolbar-row">
            <button class="tool-btn refresh-btn" onclick="refresh()">🔄 刷新</button>
            <button class="tool-btn clear-btn" onclick="clearCache()">🗑️ 清空</button>
        </div>
    </div>
    
    <div id="newsList"><div class="loading">加载中...</div></div>
    <div class="footer">📰 点击展开详情 | 自动清理过期缓存</div>
    <script>
        let currentType = 'all';
        let expandedId = null;
        
        function showToast(msg) {
            const toast = document.getElementById('toast');
            toast.textContent = msg;
            toast.style.display = 'block';
            setTimeout(() => {
                toast.style.display = 'none';
            }, 2000);
        }
        
        function toggleNews(id) {
            const full = document.getElementById('full_' + id);
            const preview = document.getElementById('preview_' + id);
            const btn = document.getElementById('btn_' + id);
            
            if (full.classList.contains('show')) {
                full.classList.remove('show');
                preview.style.display = '-webkit-box';
                btn.textContent = '▼';
            } else {
                if (expandedId !== null) {
                    const prevFull = document.getElementById('full_' + expandedId);
                    const prevPreview = document.getElementById('preview_' + expandedId);
                    const prevBtn = document.getElementById('btn_' + expandedId);
                    if (prevFull) {
                        prevFull.classList.remove('show');
                        prevPreview.style.display = '-webkit-box';
                        prevBtn.textContent = '▼';
                    }
                }
                full.classList.add('show');
                preview.style.display = 'none';
                btn.textContent = '▲';
                expandedId = id;
            }
        }
        
        async function loadNews(type = 'all') {
            const container = document.getElementById('newsList');
            container.innerHTML = '<div class="loading">加载中...</div>';
            try {
                const resp = await fetch(`/api/news?type=${type}`);
                const news = await resp.json();
                if (!news || news.length === 0) {
                    container.innerHTML = '<div class="loading">暂无新闻</div>';
                    return;
                }
                document.getElementById('count').textContent = news.length;
                document.getElementById('updateTime').textContent = '🕐 ' + new Date().toLocaleTimeString();
                
                expandedId = null;
                const typeMap = {
                    'stock': '股市',
                    'company': '公司',
                    'tech': '科技',
                    'finance': '融资'
                };
                container.innerHTML = '<div class="news-grid">' + news.map((item, index) => {
                    const id = 'news_' + index + '_' + Date.now();
                    const hasContent = item.content && item.content.length > 0;
                    const typeLabel = typeMap[item.type] || item.type;
                    return `
                        <div class="news-item" onclick="toggleNews('${id}')">
                            <div class="title">${escapeHtml(item.title)}</div>
                            <div id="preview_${id}" class="content-preview">${hasContent ? escapeHtml(item.content) : ''}</div>
                            <div id="full_${id}" class="content-full">${hasContent ? escapeHtml(item.content) : '暂无详细内容'}</div>
                            <div class="meta">
                                <span class="source-tag">${escapeHtml(item.source || '')}</span>
                                <span>${escapeHtml(item.publish_time || '').slice(5, 16)}</span>
                                ${item.type && item.type !== 'all' ? `<span class="tag ${item.type}">${typeLabel}</span>` : ''}
                                ${hasContent ? `<span class="expand-btn" id="btn_${id}">▼</span>` : ''}
                            </div>
                        </div>
                    `;
                }).join('') + '</div>';
            } catch(e) {
                container.innerHTML = '<div class="loading">加载失败，请重试</div>';
            }
        }
        
        function escapeHtml(text) {
            if (!text) return '';
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        async function refresh() {
            const btn = document.querySelector('.refresh-btn');
            btn.textContent = '⏳';
            btn.disabled = true;
            try {
                const resp = await fetch('/api/refresh', { method: 'POST' });
                const result = await resp.json();
                if (result.added > 0) {
                    showToast('✅ 新增 ' + result.added + ' 条资讯');
                } else {
                    showToast('💡 暂无新资讯更新');
                }
                await loadNews(currentType);
            } catch(e) {
                showToast('❌ 刷新请求失败');
            } finally {
                btn.textContent = '🔄 刷新';
                btn.disabled = false;
            }
        }
        
        async function clearCache() {
            if (!confirm('确定清空全部新闻缓存吗？')) return;
            const btn = document.querySelector('.clear-btn');
            btn.textContent = '⏳';
            btn.disabled = true;
            try {
                await fetch('/api/clear', { method: 'POST' });
                showToast('🗑️ 缓存已全部清空');
                await loadNews(currentType);
            } catch(e) {
                showToast('❌ 清空缓存失败');
            } finally {
                btn.textContent = '🗑️ 清空';
                btn.disabled = false;
            }
        }
        
        document.querySelectorAll('.filter-btn').forEach(btn => {
            btn.onclick = function() {
                document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
                this.classList.add('active');
                currentType = this.dataset.type;
                loadNews(currentType);
            };
        });
        
        loadNews('all');
        // 30分钟自动刷新
        setInterval(refresh, 1800000);
    </script>
</body>
</html>
"""

# ==================== HTTP服务 ====================
class HttpHandler(BaseHTTPRequestHandler):
    def _send_json(self, data):
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json;charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
        except BrokenPipeError:
            pass
        except Exception as e:
            logger.error(f"发送数据失败：{e}")

    def do_GET(self):
        url_parse = urllib.parse.urlparse(self.path)
        path = url_parse.path
        query = urllib.parse.parse_qs(url_parse.query)

        try:
            if path == '/':
                self.send_response(200)
                self.send_header("Content-Type", "text/html;charset=utf-8")
                self.end_headers()
                self.wfile.write(HTML_TEMPLATE.encode('utf-8'))
            elif path == '/api/news':
                news_type = query.get('type', ['all'])[0]
                data = storage.get_news(news_type)
                self._send_json(data)
            elif path == '/api/stats':
                self._send_json(storage.get_stats())
            elif path == '/api/health':
                self._send_json({"status": "ok", "time": datetime.datetime.now().isoformat()})
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            logger.error(f"处理GET请求失败：{e}")

    def do_POST(self):
        try:
            if self.path == '/api/refresh':
                news = fetch_all_news()
                add_num = storage.add_news(news)
                self._send_json({
                    "status": "success",
                    "added": add_num,
                    "total": storage.get_stats()["total"]
                })
            elif self.path == '/api/clear':
                cleared = storage.clear_all_cache()
                self._send_json({
                    "status": "success",
                    "cleared": cleared
                })
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            logger.error(f"处理POST请求失败：{e}")

    def log_message(self, format, *args):
        return

# ==================== 启动入口 ====================
if __name__ == '__main__':
    # 初始化存储并首次抓取
    storage = NewsStorage()
    logger.info("🔄 初始全量抓取6大财经资讯源...")
    news = fetch_all_news()
    added = storage.add_news(news)
    logger.info(f"✅ 本次新增 {added} 条，缓存共 {storage.get_stats()['total']} 条新闻")
    
    print("=" * 65)
    print("📈 财经科技新闻聚合服务（Render部署版）")
    print("📊 数据源：东方财富、36氪、新浪RSS、证券时报、同花顺、华尔街见闻")
    print(f"🌐 服务端口: {PORT}")
    print("=" * 65)
    
    stats = storage.get_stats()
    print(f"\n📊 当前新闻统计：")
    print(f"  • 总条数：{stats['total']} 条")
    print(f"  • 分类数量：{stats['by_type']}")
    print(f"  • 各来源条数：{stats['by_source']}")
    print("\n" + "=" * 65)
    print("✅ 服务启动完成")

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer(("0.0.0.0", PORT), HttpHandler) as httpd:
        httpd.serve_forever()
