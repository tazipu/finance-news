# -*- coding: utf-8 -*-
"""
财经新闻聚合系统 - 多环境适配版【本地/QPython/Render】
数据源：36氪RSS、新浪RSS、证券时报、同花顺、华尔街见闻
"""
import datetime
import hashlib
import json
import os
import time
import logging
import re
import threading
from typing import List, Dict, Optional
from collections import defaultdict
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
import socketserver
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==================== 环境检测 ====================
def detect_environment():
    """检测运行环境"""
    if os.environ.get('RENDER'):
        return 'render'
    elif os.path.exists('/data/user/0/org.qpython.qpy'):
        return 'qpython'
    else:
        return 'local'

ENV = detect_environment()
IS_RENDER = ENV == 'render'
IS_QPTHON = ENV == 'qpython'
IS_LOCAL = ENV == 'local'

# ==================== 日志配置 ====================
if IS_QPTHON:
    logging.basicConfig(level=logging.WARNING)
else:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== 全局请求配置 ====================
PORT = int(os.environ.get("PORT", 5000))

if IS_QPTHON:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
else:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

CACHE_FILE = os.path.join(SCRIPT_DIR, "news_cache.json")

MAX_NEWS = 300
REQUEST_TIMEOUT = 10
CACHE_EXPIRE_DAYS = 3
AUTO_CLEAN_INTERVAL = 7200
COMMON_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# ==================== 文本处理辅助函数 ====================

def get_first_paragraph(text: str, max_length: int = 350) -> str:
    """
    抓取标题下方首段文字，在句号、感叹号、问号处结束，不以无符号断开
    """
    if not text:
        return ''
    
    # 清理HTML标签
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    if not text:
        return ''
    
    # 句末标点（句号/感叹号/问号，含半角）
    end_chars = '。！？!?'
    # 句中停顿标点（无句末标点时，退到此并补句号）
    mid_chars = '，,、；;'
    
    # 先按换行符分割获取第一段
    paragraphs = re.split(r'\n+', text)
    first_para = paragraphs[0].strip() if paragraphs else text
    
    # 如果第一段太短（少于20个字符），尝试合并下一段
    if len(first_para) < 20 and len(paragraphs) > 1:
        first_para = first_para + ' ' + paragraphs[1].strip()
    
    if not first_para:
        return ''
    
    def cut_at_ending(s: str) -> str:
        """找到第一个句末标点并返回其前缀（含标点）"""
        for i, ch in enumerate(s):
            if ch in end_chars:
                return s[:i + 1]
        return ''
    
    def close_without_ending(s: str) -> str:
        """全文无句末标点时，退到最近停顿标点并补句号"""
        for i in range(len(s) - 1, -1, -1):
            if s[i] in mid_chars:
                return s[:i + 1] + '。'
        return s + '。'
    
    # 1) 段内在限制范围内找到第一个句末标点 → 直接截取
    head = cut_at_ending(first_para)
    if head and len(head) <= max_length:
        return head
    
    # 2) 段长超过限制 → 从限制位置向前找最近的句末标点
    if len(first_para) > max_length:
        window = first_para[:max_length]
        for i in range(len(window) - 1, -1, -1):
            if window[i] in end_chars:
                return window[:i + 1]
        # 3) 限制内无句末标点 → 向后扩展找下一个句末标点
        tail = first_para[max_length:]
        if tail:
            for i, ch in enumerate(tail):
                if ch in end_chars:
                    return first_para[:max_length + i + 1]
        # 4) 全文无句末标点 → 停顿标点处补句号
        return close_without_ending(window)
    
    # 段长在限制内且无句末标点 → 停顿标点处补句号
    return close_without_ending(first_para)

def clean_html_content(text: str) -> str:
    """清理HTML标签"""
    if not text:
        return ''
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def process_news_content(content: str, title: str = '') -> str:
    """
    处理新闻内容：清理HTML、提取第一段完整内容
    """
    if not content:
        return title[:200] if title else ''
    
    content = clean_html_content(content)
    
    if not content and title:
        content = title
    
    # 如果内容以标题开头，去除标题重复部分
    if title and content.startswith(title) and len(content) > len(title) + 10:
        content = content[len(title):].strip()
        if content and not content[0] in '，。！？、：':
            content = title + '，' + content
    
    # 获取第一段完整内容
    result = get_first_paragraph(content, 350)
    
    # 如果结果为空或太短，使用标题
    if not result or len(result) < 10:
        return title[:200] if title else ''
    
    return result

# ==================== 新闻存储模块 ====================
class NewsStorage:
    def __init__(self):
        self.news_list: List[Dict] = []
        self.news_hashes: set = set()
        self.title_hashes: set = set()  # 用于标题去重
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
                        self.title_hashes = set()
                        for news in data:
                            fingerprint = news.get('_fingerprint')
                            if fingerprint:
                                self.news_hashes.add(fingerprint)
                            title_hash = self._get_title_hash(news.get('title', ''))
                            if title_hash:
                                self.title_hashes.add(title_hash)
                    else:
                        self.news_list = data.get('news', [])
                        self.news_hashes = set(data.get('hashes', []))
                        self.title_hashes = set(data.get('title_hashes', []))
                logger.info(f"✅ 加载缓存：{len(self.news_list)}条")
                self.clean_expired_cache()
            except Exception as e:
                logger.error(f"❌ 加载缓存失败：{e}")
                self.news_list = []
                self.news_hashes = set()
                self.title_hashes = set()

    def save_cache(self):
        try:
            data = {
                'news': self.news_list[:MAX_NEWS],
                'hashes': list(self.news_hashes),
                'title_hashes': list(self.title_hashes),
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
            self.title_hashes = set()
            for news in new_list:
                fingerprint = news.get('_fingerprint')
                if fingerprint:
                    self.news_hashes.add(fingerprint)
                title_hash = self._get_title_hash(news.get('title', ''))
                if title_hash:
                    self.title_hashes.add(title_hash)
            logger.info(f"🧹 清理过期缓存：{expired_count}条")
            self.save_cache()

    def clear_all_cache(self):
        count = len(self.news_list)
        self.news_list = []
        self.news_hashes = set()
        self.title_hashes = set()
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

    def _get_title_hash(self, title: str) -> str:
        """生成标题的哈希值用于去重"""
        if not title:
            return ''
        # 清理标题中的特殊字符和数字
        clean = re.sub(r'[0-9一二三四五六七八九十百千万亿\d]', '', title)
        clean = re.sub(r'[，。！？、：；""''（）【】\s]', '', clean)
        if len(clean) < 5:  # 如果清理后太短，使用原始标题
            clean = title
        return hashlib.md5(clean.encode('utf-8')).hexdigest()

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
            
            # 检查标题是否重复（去重）
            title_hash = self._get_title_hash(item.get('title', ''))
            if title_hash and title_hash in self.title_hashes:
                continue
            
            fingerprint = self._get_fingerprint(item)
            if fingerprint not in self.news_hashes:
                item['_fingerprint'] = fingerprint
                item['_title_hash'] = title_hash
                self.news_list.append(item)
                self.news_hashes.add(fingerprint)
                if title_hash:
                    self.title_hashes.add(title_hash)
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
            content = n.get('content', '')
            if content.endswith('...'):
                content = content[:-3]
            # 展示截断：确保以句末标点结束，不以无符号断开
            if len(content) > 200:
                cut = content[:200]
                pos = -1
                for i in range(len(cut) - 1, -1, -1):
                    if cut[i] in '。！？!?':
                        pos = i
                        break
                content = cut[:pos + 1] if pos > 30 else cut
            result.append({
                'title': n.get('title', ''),
                'content': content,
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

def fetch_36kr_rss():
    """36氪RSS"""
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
            content_raw = desc.text if desc is not None else ''
            
            content = process_news_content(content_raw, title)
            
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
    """新浪RSS"""
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
            content_raw = desc.text if desc is not None else ''
            
            content = process_news_content(content_raw, title)
            
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
            content_raw = item.get("content", title)
            
            content = process_news_content(content_raw, title)
            
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
            content_raw = item.get("digest", title)
            
            content = process_news_content(content_raw, title)
            
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
        logger.error(f"❌ 同花顺抓取失败: {str(e)}")
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
            content_raw = item.get("content_text", title)
            
            content = process_news_content(content_raw, title)
            
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

# ==================== 分类和统一抓取入口 ====================

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
    """并发抓取全部5个数据源"""
    source_tasks = [
        ("36氪", fetch_36kr_rss),
        ("新浪RSS", fetch_sina_rss),
        ("证券时报", fetch_stcn),
        ("同花顺", fetch_ths),
        ("华尔街见闻", fetch_wscn)
    ]
    all_news = []
    success_count = 0

    with ThreadPoolExecutor(max_workers=5) as executor:
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
    
    if not all_news:
        logger.warning("⚠️ 所有数据源都抓取失败，返回空列表")
    
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
            min-height: 90px;
        }
        .news-item:hover { background: #f8f9ff; transform: translateY(-1px); box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
        .news-item .title { 
            font-size: 14px; 
            font-weight: 600; 
            margin-bottom: 6px;
            line-height: 1.5;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
        }
        .news-item .content-preview { 
            font-size: 13px; 
            color: #555; 
            line-height: 1.6;
            display: -webkit-box;
            -webkit-line-clamp: 3;
            -webkit-box-orient: vertical;
            overflow: hidden;
            flex: 1;
            margin-bottom: 6px;
        }
        .news-item .content-full {
            font-size: 13px;
            color: #333;
            line-height: 1.8;
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
        .news-item .expand-btn:hover { color: #764ba2; }
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
        .env-badge {
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
        <div class="api-info">📰 36氪 + 新浪RSS + 证券时报 + 同花顺 + 华尔街见闻</div>
        <div class="env-badge" id="envBadge">🚀 加载中...</div>
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
        
        async function detectEnv() {
            try {
                const resp = await fetch('/api/env');
                const data = await resp.json();
                document.getElementById('envBadge').textContent = '🚀 ' + data.env;
            } catch(e) {
                document.getElementById('envBadge').textContent = '🚀 本地运行';
            }
        }
        detectEnv();
        
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
                    container.innerHTML = '<div class="loading">暂无新闻<br><small>请点击"刷新"按钮手动抓取</small></div>';
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
            elif path == '/api/env':
                self._send_json({"env": ENV.upper(), "render": IS_RENDER, "qpython": IS_QPTHON})
            elif path == '/api/health':
                self._send_json({"status": "ok", "time": datetime.datetime.now().isoformat(), "env": ENV})
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
    storage = NewsStorage()
    
    env_names = {
        'render': '☁️ Render云平台',
        'qpython': '📱 QPython手机端',
        'local': '💻 本地电脑'
    }
    
    print("=" * 65)
    print(f"📈 财经科技新闻聚合服务 ({env_names.get(ENV, ENV)})")
    print("📊 数据源：36氪、新浪RSS、证券时报、同花顺、华尔街见闻")
    print(f"🌐 服务端口: {PORT}")
    print(f"📁 缓存路径: {CACHE_FILE}")
    print("=" * 65)
    
    stats = storage.get_stats()
    if stats['total'] > 0:
        print(f"📊 使用缓存数据：{stats['total']} 条新闻")
        print(f"  • 分类数量：{stats['by_type']}")
        print(f"  • 各来源条数：{stats['by_source']}")
    else:
        print("🔄 首次运行，抓取新闻...")
        news = fetch_all_news()
        if news:
            added = storage.add_news(news)
            logger.info(f"✅ 本次新增 {added} 条")
            stats = storage.get_stats()
            print(f"\n📊 当前新闻统计：")
            print(f"  • 总条数：{stats['total']} 条")
            print(f"  • 分类数量：{stats['by_type']}")
            print(f"  • 各来源条数：{stats['by_source']}")
        else:
            print("⚠️ 所有数据源抓取失败，请检查网络连接")
    
    print("\n" + "=" * 65)
    
    if IS_QPTHON:
        print("📱 QPython环境访问地址：")
        print("  • http://127.0.0.1:5000")
        print("  • http://localhost:5000")
        print("  • 在手机浏览器输入上述地址访问")
    elif IS_RENDER:
        print("☁️ Render部署完成，等待外部访问...")
    else:
        print("💻 本地访问地址：http://127.0.0.1:5000")
    
    print("✅ 服务启动完成，Ctrl+C 终止服务")
    print("=" * 65)

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer(("0.0.0.0", PORT), HttpHandler) as httpd:
        httpd.serve_forever()
