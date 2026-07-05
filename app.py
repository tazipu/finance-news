# -*- coding: utf-8 -*-
"""
财经新闻聚合系统 - 按钮第二排右下角
"""
import datetime
import hashlib
import json
import os
import time
import logging
import random
import re
import xml.etree.ElementTree as ET
import threading
from typing import List, Dict, Optional
from collections import defaultdict
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
import socketserver
import urllib.parse

# 日志配置
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 安卓存储路径
try:
    CACHE_FILE = os.path.join(os.environ.get('HOME', '/sdcard'), "news_cache.json")
except:
    CACHE_FILE = "/data/data/ru.iiec.pydroid3/news_cache.json"

MAX_NEWS = 500
REQUEST_TIMEOUT = 10
CACHE_EXPIRE_DAYS = 7
AUTO_CLEAN_INTERVAL = 3600

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
                                days_diff = (now - dt).days
                                if days_diff <= CACHE_EXPIRE_DAYS:
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
                logger.info(f"🗑️ 已删除缓存文件")
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
                    logger.info(f"🔄 自动清理完成，当前缓存：{len(self.news_list)}条")
                except Exception as e:
                    logger.error(f"❌ 自动清理失败：{e}")
        
        thread = threading.Thread(target=auto_clean, daemon=True)
        thread.start()
        logger.info(f"✅ 自动清理已启动（间隔{AUTO_CLEAN_INTERVAL//60}分钟，保留{CACHE_EXPIRE_DAYS}天）")

    def _get_fingerprint(self, news_item: Dict) -> str:
        text = f"{news_item.get('title', '')}{news_item.get('content', '')[:50]}"
        return hashlib.md5(text.encode('utf-8')).hexdigest()

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
        return news

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
        url = "https://36kr.com/feed"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        
        for item in root.findall('.//item')[:25]:
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
                content = re.sub(r'<[^>]+>', '', content)[:200]
            
            news_list.append({
                'title': title,
                'content': content,
                'source': '36氪',
                'publish_time': pub_time,
                'type': classify_news(title),
                'url': item.find('link').text if item.find('link') is not None else ''
            })
        logger.info(f"✅ 36氪：{len(news_list)}条")
    except Exception as e:
        logger.error(f"❌ 36氪失败：{e}")
    return news_list

def fetch_sina_api():
    """新浪财经API"""
    news_list = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
    }
    try:
        categories = [
            {'pageid': 153, 'lid': 2509},
            {'pageid': 153, 'lid': 2510},
        ]
        for cat in categories:
            try:
                params = {'pageid': cat['pageid'], 'lid': cat['lid'], 'num': 20, 'version': '1.0'}
                resp = requests.get("https://feed.sina.com.cn/api/roll/get", params=params, 
                                  headers=headers, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
                items = data.get('result', {}).get('data', [])
                for item in items:
                    title = item.get('title', '').strip()
                    if title:
                        news_list.append({
                            'title': title,
                            'content': item.get('content', '') or item.get('intro', ''),
                            'source': '新浪财经',
                            'publish_time': item.get('ctime', '') or item.get('time', ''),
                            'type': classify_news(title),
                            'url': item.get('url', '')
                        })
            except:
                pass
            time.sleep(0.2)
        logger.info(f"✅ 新浪财经：{len(news_list)}条")
    except Exception as e:
        logger.error(f"❌ 新浪财经失败：{e}")
    return news_list

def fetch_sina_rss():
    """新浪RSS"""
    news_list = []
    try:
        url = "https://rss.sina.com.cn/finance/rollnews.xml"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
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
                content = re.sub(r'<[^>]+>', '', content)[:200]
            
            news_list.append({
                'title': title,
                'content': content,
                'source': '新浪RSS',
                'publish_time': pub_time,
                'type': classify_news(title),
                'url': item.find('link').text if item.find('link') is not None else ''
            })
        logger.info(f"✅ 新浪RSS：{len(news_list)}条")
    except Exception as e:
        logger.error(f"❌ 新浪RSS失败：{e}")
    return news_list

def generate_mock_news(count=12) -> List[Dict]:
    """生成模拟数据"""
    news_list = []
    now = datetime.datetime.now()
    
    mock_templates = [
        {'title': '🚀 字节跳动推出AI新应用，日活突破百万', 'content': '字节跳动最新推出的AI应用在上线首周即获得百万日活用户。', 'type': 'tech'},
        {'title': '💰 新能源车企获得新一轮融资，估值超百亿', 'content': '某头部新能源车企完成新一轮融资，投后估值超过100亿元。', 'type': 'finance'},
        {'title': '📱 华为发布新旗舰手机，搭载自研芯片', 'content': '华为最新旗舰手机正式发布，搭载全新自研麒麟芯片。', 'type': 'tech'},
        {'title': '📈 A股三大指数收涨，成交额突破万亿', 'content': '沪指涨0.8%报3280点，深成指涨1.2%报11200点。', 'type': 'stock'},
        {'title': '💡 人工智能芯片公司获巨额投资', 'content': '某人工智能芯片公司完成数亿元融资，加速产品研发。', 'type': 'tech'},
        {'title': '🌍 美团布局海外市场，首站落地东南亚', 'content': '美团宣布正式进入东南亚市场，首站选择新加坡。', 'type': 'company'},
        {'title': '📊 公募基金规模突破30万亿元', 'content': '中国公募基金总规模突破30万亿元，创历史新高。', 'type': 'finance'},
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

def fetch_all_news() -> List[Dict]:
    """获取所有可用数据源"""
    all_news = []
    
    sources = [
        ('36氪', fetch_36kr_rss),
        ('新浪财经API', fetch_sina_api),
        ('新浪财经RSS', fetch_sina_rss),
    ]
    
    success_count = 0
    for name, func in sources:
        try:
            logger.info(f"🔄 正在获取 {name}...")
            news = func()
            if news:
                all_news.extend(news)
                success_count += 1
            time.sleep(0.3)
        except Exception as e:
            logger.error(f"❌ {name} 失败：{e}")
    
    logger.info(f"✅ 成功 {success_count}/{len(sources)} 个数据源")
    
    if len(all_news) < 20:
        logger.info(f"📝 补充模拟数据")
        mock_news = generate_mock_news(15)
        all_news.extend(mock_news)
    
    return all_news

def classify_news(title):
    """新闻分类"""
    text = title.lower()
    
    # 股市/行情
    if any(kw in text for kw in ['涨停', '跌停', 'a股', '港股', '美股', '指数', '沪指', '深成指', '创业板', '上证', '收盘', '开盘']):
        return 'stock'
    
    # 公司/企业
    if any(kw in text for kw in ['公司', '企业', '集团', '财报', '业绩', '营收', '净利润', '分红', '公告', '上市']):
        return 'company'
    
    # 科技/创新
    if any(kw in text for kw in ['ai', '人工智能', '芯片', '科技', '研发', '算法', '大模型', '数据', '智能', '机器人']):
        return 'tech'
    
    # 融资/创投
    if any(kw in text for kw in ['融资', '投资', '估值', '股权', '天使轮', 'a轮', 'b轮', 'c轮', '千万', '亿', '募资']):
        return 'finance'
    
    return 'all'

# 初始化
storage = NewsStorage()
logger.info("🔄 初始抓取...")
news = fetch_all_news()
added = storage.add_news(news)
logger.info(f"✅ 新增 {added} 条，共 {storage.get_stats()['total']} 条新闻")

# ==================== HTML页面 ====================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>📈 财经科技新闻</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, sans-serif; background: #f0f2f5; padding: 12px; }
        .header { background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 16px 20px; border-radius: 12px; margin-bottom: 12px; }
        .header h1 { font-size: 20px; }
        .header .stats { font-size: 12px; opacity: 0.9; margin-top: 4px; }
        
        /* 分类容器 */
        .filters-wrapper {
            margin-bottom: 12px;
        }
        
        /* 第一排：分类按钮 */
        .filter-row {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
        }
        
        /* 第二排：工具按钮（右下角） */
        .toolbar-row {
            display: flex;
            justify-content: flex-end;
            gap: 6px;
            margin-top: 6px;
        }
        
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
        .filter-btn.active {
            background: #667eea;
            color: white;
            border-color: #667eea;
        }
        .filter-btn .icon { margin-right: 2px; }
        
        /* 工具按钮 - 与分类按钮大小一致 */
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
        .tool-btn.refresh-btn {
            border-color: #667eea;
            color: #667eea;
        }
        .tool-btn.refresh-btn:hover {
            background: #667eea;
            color: white;
        }
        .tool-btn.clear-btn {
            border-color: #ef4444;
            color: #ef4444;
        }
        .tool-btn.clear-btn:hover {
            background: #ef4444;
            color: white;
        }
        
        .news-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
        }
        @media (max-width: 480px) {
            .news-grid {
                grid-template-columns: 1fr;
            }
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
            font-size: 13px; 
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
        .news-item .content-full.show {
            display: block;
            animation: fadeIn 0.3s;
        }
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
        .news-item .expand-btn {
            font-size: 11px;
            color: #667eea;
            user-select: none;
        }
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
    </style>
</head>
<body>
    <div id="toast" class="toast"></div>
    <div class="header">
        <h1>📈 财经科技新闻</h1>
        <div class="stats">共 <span id="count">0</span> 条 <span class="update-time" id="updateTime"></span></div>
        <div class="api-info">📰 36氪 + 新浪财经</div>
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
    <div class="footer">📰 点击展开 | 自动清理7天前缓存</div>
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
                                <span>${escapeHtml(item.source || '')}</span>
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
                    showToast('✅ 新增 ' + result.added + ' 条');
                } else {
                    showToast('💡 没有新新闻');
                }
                await loadNews(currentType);
            } catch(e) {
                showToast('❌ 刷新失败');
            } finally {
                btn.textContent = '🔄 刷新';
                btn.disabled = false;
            }
        }
        
        async function clearCache() {
            if (!confirm('确定要清空所有缓存吗？')) return;
            
            const btn = document.querySelector('.clear-btn');
            btn.textContent = '⏳';
            btn.disabled = true;
            try {
                const resp = await fetch('/api/clear', { method: 'POST' });
                const result = await resp.json();
                showToast('🗑️ 已清空 ' + result.cleared + ' 条');
                await loadNews(currentType);
            } catch(e) {
                showToast('❌ 清空失败');
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
        setInterval(refresh, 300000);
    </script>
</body>
</html>
"""

# ==================== HTTP服务 ====================
class HttpHandler(BaseHTTPRequestHandler):
    def _send_json(self, data):
        self.send_response(200)
        self.send_header("Content-Type", "application/json;charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))

    def do_GET(self):
        url_parse = urllib.parse.urlparse(self.path)
        path = url_parse.path
        query = urllib.parse.parse_qs(url_parse.query)

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

    def do_POST(self):
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

    def log_message(self, format, *args):
        return

# ==================== 启动 ====================
if __name__ == '__main__':
    print("=" * 60)
    print("📈 财经科技新闻聚合服务")
    print("📊 数据源：36氪 + 新浪财经")
    print("🌐 访问: http://127.0.0.1:5000")
    print(f"🧹 自动清理：保留{CACHE_EXPIRE_DAYS}天，间隔{AUTO_CLEAN_INTERVAL//60}分钟")
    print("=" * 60)
    
    stats = storage.get_stats()
    print(f"\n📊 新闻统计：")
    print(f"  • 总数：{stats['total']} 条")
    print(f"  • 分类：{stats['by_type']}")
    print(f"  • 来源：{stats['by_source']}")
    print("\n" + "=" * 60)

    socketserver.ThreadingTCPServer.allow_reuse_address = True
port = int(os.environ.get('PORT', 5000))
with socketserver.ThreadingTCPServer(("0.0.0.0", port), HttpHandler) as httpd:
    httpd.serve_forever()