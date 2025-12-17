import gzip
import json
import re
import time
from collections import deque
from datetime import datetime
from importlib import import_module
from queue import Empty, Queue
from threading import Lock
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from calibre import random_user_agent
from calibre.ebooks.metadata.book.base import Metadata
from calibre.ebooks.metadata.sources.base import Option, Source
from calibre.utils.localization import _


PROVIDER_NAME = 'Calibre-XSH Catalog'
PROVIDER_ID = 'calibre_xsh'
PROVIDER_VERSION = (1, 0, 0)
DEFAULT_BASE_URL = 'https://api.xiaoshuohub.com/api/v1'
DEFAULT_SEARCH_BASE_URL = DEFAULT_BASE_URL
DEFAULT_TIMEOUT = 20
BUCKET_WINDOW_SECONDS = 600
PLACEHOLDER_AUTHORS = {'未知', '佚名', '无名氏', '无名', 'unknown', 'n/a', 'na', 'unknown author'}


class XshCatalogError(Exception):
    pass


class HourBucketRateLimiter:
    def __init__(self, hour_limit, bucket_limit, bucket_window=BUCKET_WINDOW_SECONDS):
        self.hour_limit = max(int(hour_limit or 1), 1)
        fallback_bucket = max(self.hour_limit // 6, 1)
        self.bucket_limit = max(int(bucket_limit or fallback_bucket), 1)
        self.bucket_window = max(int(bucket_window), 1)
        self.hour_window = 3600
        self.timestamps = deque()
        self.lock = Lock()

    def wait_for_slot(self):
        while True:
            with self.lock:
                now = time.time()
                self._trim(now)
                if self._has_capacity(now):
                    self.timestamps.append(now)
                    return
                delay = self._next_delay(now)
            if delay <= 0:
                continue
            time.sleep(delay)

    def _trim(self, now):
        while self.timestamps and now - self.timestamps[0] >= self.hour_window:
            self.timestamps.popleft()

    def _has_capacity(self, now):
        if len(self.timestamps) < self.hour_limit:
            bucket_count, _ = self._bucket_info(now)
            return bucket_count < self.bucket_limit
        return False

    def _bucket_info(self, now):
        count = 0
        oldest = None
        for ts in reversed(self.timestamps):
            if now - ts <= self.bucket_window:
                count += 1
                oldest = ts
            else:
                break
        return count, oldest

    def _next_delay(self, now):
        delay = 0.0
        if len(self.timestamps) >= self.hour_limit:
            delay = max(delay, self.hour_window - (now - self.timestamps[0]))
        bucket_count, oldest = self._bucket_info(now)
        if bucket_count >= self.bucket_limit and oldest is not None:
            delay = max(delay, self.bucket_window - (now - oldest))
        return max(delay, 0.01)


class XshCatalogClient:
    def __init__(self, base_url, api_key, timeout, rate_limiter):
        self.base_url = base_url.rstrip('/')
        parsed = urlparse(self.base_url)
        root = f'{parsed.scheme}://{parsed.netloc}' if parsed.scheme and parsed.netloc else self.base_url
        self.base_root = root.rstrip('/')
        self.base_path = parsed.path.rstrip('/') if parsed.path and parsed.path != '/' else ''
        self.api_key = (api_key or '').strip()
        self.timeout = timeout
        self.rate_limiter = rate_limiter

    def search_books(self, keyword, page_size):
        params = {
            'keyword': keyword,
            'page_num': 1,
            'page_size': page_size,
            'sort_by': 'standard_points',
            'sort_order': 'DESC'
        }
        payload = self._request('/api/v1/books', params=params)
        return ((payload or {}).get('data') or {}).get('records') or []

    def get_book(self, book_id, fields='all'):
        payload = self._request(f'/api/v1/books/{book_id}', params={'fields': fields or 'all'})
        return (payload or {}).get('data')

    def _request(self, path, params=None):
        if self.rate_limiter:
            self.rate_limiter.wait_for_slot()
        url = self._build_url(path, params)
        headers = self._build_headers()
        req = Request(url, headers=headers, method='GET')
        try:
            with urlopen(req, timeout=self.timeout) as resp:
                data = resp.read()
                if resp.info().get('Content-Encoding') == 'gzip':
                    data = gzip.decompress(data)
                encoding = resp.headers.get_content_charset() or 'utf-8'
                text = data.decode(encoding)
                payload = json.loads(text)
        except HTTPError as err:
            raise XshCatalogError(_('HTTP错误: %(code)s') % {'code': err.code})
        except URLError as err:
            raise XshCatalogError(_('网络错误: %(reason)s') % {'reason': err.reason})
        except json.JSONDecodeError as err:
            raise XshCatalogError(_('解析响应失败: %(msg)s') % {'msg': err.msg})

        if isinstance(payload, dict):
            code = payload.get('code')
            if code not in (0, None):
                message = payload.get('message') or _('请求失败')
                raise XshCatalogError(f'{message} (code={code})')
            return payload
        raise XshCatalogError(_('未知响应格式'))

    def _build_headers(self):
        headers = {
            'Accept': 'application/json',
            'User-Agent': random_user_agent(),
        }
        if self.api_key:
            headers['X-Api-Key'] = self.api_key
        return headers

    def _build_url(self, path, params=None):
        path = path or ''
        if path.startswith('/') and self.base_path and path.startswith(self.base_path):
            path = path[len(self.base_path):]
            if not path.startswith('/'):
                path = '/' + path

        parts = []
        if self.base_path:
            parts.append(self.base_path.lstrip('/'))
        if path:
            parts.append(path.lstrip('/'))

        full_path = '/'.join(filter(None, parts))
        if full_path:
            url = f'{self.base_root}/{full_path}'
        else:
            url = self.base_root

        if params:
            encoded = urlencode({k: v for k, v in params.items() if v not in (None, '')})
            if encoded:
                return f'{url}?{encoded}'
        return url


class XshSearchClient:
    def __init__(self, base_url, api_key, timeout, rate_limiter):
        self.base_url = base_url.rstrip('/')
        parsed = urlparse(self.base_url)
        root = f'{parsed.scheme}://{parsed.netloc}' if parsed.scheme and parsed.netloc else self.base_url
        self.base_root = root.rstrip('/')
        self.base_path = parsed.path.rstrip('/') if parsed.path and parsed.path != '/' else ''
        self.api_key = (api_key or '').strip()
        self.timeout = timeout
        self.rate_limiter = rate_limiter

    def search_books(self, keyword, page_size):
        params = {
            'q': keyword,
            'page_num': 1,
            'page_size': page_size,
            'sort_by': 'relevance',
            'sort_order': 'desc'
        }
        payload = self._request('/api/v1/search/books', params=params)
        return ((payload or {}).get('data') or {}).get('records') or []

    def _request(self, path, params=None):
        if self.rate_limiter:
            self.rate_limiter.wait_for_slot()
        url = self._build_url(path, params)
        headers = self._build_headers()
        req = Request(url, headers=headers, method='GET')
        try:
            with urlopen(req, timeout=self.timeout) as resp:
                data = resp.read()
                if resp.info().get('Content-Encoding') == 'gzip':
                    data = gzip.decompress(data)
                encoding = resp.headers.get_content_charset() or 'utf-8'
                text = data.decode(encoding)
                payload = json.loads(text)
        except HTTPError as err:
            raise XshCatalogError(_('HTTP错误: %(code)s') % {'code': err.code})
        except URLError as err:
            raise XshCatalogError(_('网络错误: %(reason)s') % {'reason': err.reason})
        except json.JSONDecodeError as err:
            raise XshCatalogError(_('解析响应失败: %(msg)s') % {'msg': err.msg})

        if isinstance(payload, dict):
            code = payload.get('code')
            if code not in (0, None):
                message = payload.get('message') or _('请求失败')
                raise XshCatalogError(f'{message} (code={code})')
            return payload
        raise XshCatalogError(_('未知响应格式'))

    def _build_headers(self):
        headers = {
            'Accept': 'application/json',
            'User-Agent': random_user_agent(),
        }
        if self.api_key:
            headers['X-Api-Key'] = self.api_key
        return headers

    def _build_url(self, path, params=None):
        path = path or ''
        if path.startswith('/') and self.base_path and path.startswith(self.base_path):
            path = path[len(self.base_path):]
            if not path.startswith('/'):
                path = '/' + path

        parts = []
        if self.base_path:
            parts.append(self.base_path.lstrip('/'))
        if path:
            parts.append(path.lstrip('/'))

        full_path = '/'.join(filter(None, parts))
        if full_path:
            url = f'{self.base_root}/{full_path}'
        else:
            url = self.base_root

        if params:
            encoded = urlencode({k: v for k, v in params.items() if v not in (None, '')})
            if encoded:
                return f'{url}?{encoded}'
        return url


class CalibreXsh(Source):
    name = PROVIDER_NAME
    description = _('从XiaoShuoHub目录服务下载书籍元数据和封面。')
    supported_platforms = ['windows', 'osx', 'linux']
    author = 'luofengyuan'
    version = PROVIDER_VERSION
    minimum_calibre_version = (5, 0, 0)
    capabilities = frozenset(['identify', 'cover'])
    touched_fields = frozenset([
        'title', 'authors', 'publisher', 'comments', 'tags', 'rating',
        'identifier:' + PROVIDER_ID
    ])
    options = (
        Option('api_access_key', 'string', '',
               _('访问密钥'), _('由服务端分配的API Key（可在 https://www.xiaoshuohub.com/my/overview 获取）。')),
        Option('requests_per_hour', 'number', 60,
               _('每小时请求上限'), _('额度以官网查询为准，插件会自动按1/6设置10分钟限速。')),
        Option('page_size', 'number', 5,
               _('单次拉取数量'), _('识别时单次检索的最大书籍条数。')),
        Option('search_with_author', 'bool', True,
               _('搜索时包含作者'), _('自动把作者名拼接进关键词以提高命中率。')),
        Option('request_timeout', 'number', DEFAULT_TIMEOUT,
               _('请求超时(秒)'), _('HTTP请求的超时时间。')),
    )

    def __init__(self, *args, **kwargs):
        Source.__init__(self, *args, **kwargs)
        self.catalog_client = None
        self.search_client = None
        self.rate_limiter = None
        self.page_size = 5
        self.timeout = DEFAULT_TIMEOUT
        self.search_with_author = True
        self._init_client_from_prefs()

    def _init_client_from_prefs(self):
        hour_limit = max(int(self.prefs.get('requests_per_hour') or 60), 1)
        bucket_limit = max(hour_limit // 6, 1)
        self.timeout = max(int(self.prefs.get('request_timeout') or DEFAULT_TIMEOUT), 5)
        self.page_size = max(int(self.prefs.get('page_size') or 5), 1)
        self.search_with_author = bool(self.prefs.get('search_with_author'))
        self.rate_limiter = HourBucketRateLimiter(hour_limit, bucket_limit)
        api_key = (self.prefs.get('api_access_key') or '').strip()
        self.catalog_client = XshCatalogClient(DEFAULT_BASE_URL, api_key, self.timeout, self.rate_limiter)
        self.search_client = XshSearchClient(DEFAULT_SEARCH_BASE_URL, api_key, self.timeout, self.rate_limiter)

    def identify(self, log, result_queue, abort, title=None, authors=None,
                 identifiers=None, timeout=DEFAULT_TIMEOUT):
        identifiers = identifiers or {}
        book_id = self._get_identifier(identifiers)
        if abort.is_set():
            return
        if book_id:
            record = self._load_by_id(book_id, log)
            if record:
                self._emit_metadata(record, result_queue, log, abort)
                return
        inline_title, inline_authors, inline_found = self._extract_inline_title_and_authors(title)
        if inline_found:
            title = inline_title
            authors = inline_authors
        queries = self._build_queries(title, authors, identifiers, force_author=inline_found)
        if not queries:
            log.info('Calibre-XSH: 缺少可用于搜索的关键词。')
            return
        if not self.search_client:
            log.error('Calibre-XSH: 搜索客户端未初始化。')
            return
        records = []
        for idx, query in enumerate(queries):
            if abort.is_set():
                return
            try:
                records = self.search_client.search_books(query, self.page_size)
            except XshCatalogError as err:
                log.error(f'Calibre-XSH: 搜索失败，原因: {err}')
                return
            if records:
                if len(records) > self.page_size:
                    records = records[:self.page_size]
                if idx > 0:
                    log.info('Calibre-XSH: 含作者的关键词未命中，已回退到仅书名搜索。')
                break
            if idx < len(queries) - 1:
                log.info('Calibre-XSH: 关键词“%s”未命中，尝试下一组。', query)
        if not records:
            log.info('Calibre-XSH: 未找到任何候选记录。')
            return
        for record in records:
            if abort.is_set():
                break
            normalized = self._normalize_record(record)
            book_id = normalized.get('book_id') or normalized.get('id')
            if book_id:
                detail = self._load_by_id(book_id, log, fields='cover,introduction,all')
                if detail:
                    detail = self._normalize_record(detail)
                    # 覆盖/补全封面、简介、标签、分类及状态、评分等富字段
                    for key in (
                        'cover_url', 'introduction', 'categories', 'tags', 'source_site',
                        'status', 'word_count', 'chapter_count', 'average_rating', 'rating_count',
                        'publisher', 'official_url', 'created_at', 'updated_at'
                    ):
                        if detail.get(key) not in (None, ''):
                            normalized[key] = detail[key]
            self._emit_metadata(normalized, result_queue, log, abort)

    def is_customizable(self):
        return True

    def config_widget(self):
        QtWidgets = self._load_qt_widgets()

        class _Config(QtWidgets.QWidget):
            def __init__(self, plugin):
                QtWidgets.QWidget.__init__(self)
                self.plugin = plugin
                self.prefs = plugin.prefs

                layout = QtWidgets.QFormLayout(self)

                api_key = (self.prefs.get('api_access_key') or '').strip()
                self.key_edit = QtWidgets.QLineEdit(api_key)
                if hasattr(QtWidgets.QLineEdit, 'PasswordEchoOnEdit'):
                    self.key_edit.setEchoMode(QtWidgets.QLineEdit.PasswordEchoOnEdit)
                else:
                    self.key_edit.setEchoMode(QtWidgets.QLineEdit.Normal)
                layout.addRow(_('访问密钥'), self.key_edit)

                self.key_hint = QtWidgets.QLabel(_('密钥可在 https://www.xiaoshuohub.com/my/overview 获取'))
                layout.addRow('', self.key_hint)

                hour_value = int(self.prefs.get('requests_per_hour') or 60)
                self.hour_spin = QtWidgets.QSpinBox()
                self.hour_spin.setRange(1, 100000)
                self.hour_spin.setValue(hour_value)
                layout.addRow(_('每小时请求上限'), self.hour_spin)

                self.bucket_hint = QtWidgets.QLabel()
                layout.addRow(_('每10分钟请求上限'), self.bucket_hint)

                self.page_spin = QtWidgets.QSpinBox()
                self.page_spin.setRange(1, 50)
                self.page_spin.setValue(int(self.prefs.get('page_size') or 5))
                layout.addRow(_('单次拉取数量'), self.page_spin)

                self.timeout_spin = QtWidgets.QSpinBox()
                self.timeout_spin.setRange(5, 120)
                self.timeout_spin.setValue(int(self.prefs.get('request_timeout') or DEFAULT_TIMEOUT))
                layout.addRow(_('请求超时(秒)'), self.timeout_spin)

                self.author_checkbox = QtWidgets.QCheckBox(_('搜索时包含作者'))
                self.author_checkbox.setChecked(bool(self.prefs.get('search_with_author')))
                layout.addRow('', self.author_checkbox)

                self.toggle_test_btn = getattr(QtWidgets, 'QPushButton')(_('显示测试工具'))
                self.toggle_test_btn.setCheckable(True)
                self.toggle_test_btn.setChecked(False)
                layout.addRow('', self.toggle_test_btn)

                self.test_group = QtWidgets.QGroupBox(_('测试工具'))
                test_layout = QtWidgets.QFormLayout(self.test_group)
                self.test_title_edit = QtWidgets.QLineEdit('北冥神剑')
                test_layout.addRow(_('测试书名'), self.test_title_edit)

                self.test_button = getattr(QtWidgets, 'QPushButton')(_('运行测试'))
                self.test_button.clicked.connect(self.run_test)
                test_layout.addRow('', self.test_button)

                self.test_log = QtWidgets.QPlainTextEdit()
                self.test_log.setReadOnly(True)
                self.test_log.setPlaceholderText(_('点击“运行测试”以获取完整元数据响应日志'))
                self.test_log.setMinimumHeight(160)
                test_layout.addRow(_('测试日志'), self.test_log)
                self.test_group.setVisible(False)
                layout.addRow('', self.test_group)

                self.toggle_test_btn.toggled.connect(self.test_group.setVisible)
                self.toggle_test_btn.toggled.connect(self._update_toggle_text)

                self.hour_spin.valueChanged.connect(self._update_bucket_hint)
                self._update_bucket_hint()

            def _update_bucket_hint(self, *_args):
                recommended = max(self.hour_spin.value() // 6, 1)
                self.bucket_hint.setText(_('自动计算: %(value)d（按每小时限速/6，额度以官网为准）') % {'value': recommended})

            def _update_toggle_text(self, checked):
                self.toggle_test_btn.setText(_('隐藏测试工具') if checked else _('显示测试工具'))

            def save_settings(self):
                self.prefs['api_access_key'] = self.key_edit.text().strip()

                hour_limit = max(self.hour_spin.value(), 1)
                bucket_limit = max(hour_limit // 6, 1)
                self.prefs['requests_per_hour'] = hour_limit
                self.prefs['requests_per_bucket'] = bucket_limit

                self.prefs['page_size'] = self.page_spin.value()
                self.prefs['request_timeout'] = self.timeout_spin.value()
                self.prefs['search_with_author'] = self.author_checkbox.isChecked()

            def run_test(self):
                title = self.test_title_edit.text().strip()
                if not title:
                    self.test_log.setPlainText(_('请输入测试书名'))
                    return

                hour_limit = max(self.hour_spin.value(), 1)
                bucket_limit = max(hour_limit // 6, 1)
                timeout = max(self.timeout_spin.value(), 5)
                api_key = self.key_edit.text().strip()
                page_size = max(self.page_spin.value(), 1)

                limiter = HourBucketRateLimiter(hour_limit, bucket_limit)
                catalog_client = XshCatalogClient(DEFAULT_BASE_URL, api_key, timeout, limiter)
                search_client = XshSearchClient(DEFAULT_SEARCH_BASE_URL, api_key, timeout, limiter)

                self.test_button.setEnabled(False)
                self.test_log.setPlainText(_('正在请求 %(title)s ...') % {'title': title})
                try:
                    records = search_client.search_books(title, page_size)
                    if not records:
                        raise XshCatalogError(_('未查询到任何书籍记录'))
                    logs = []
                    for idx, record in enumerate(records, 1):
                        logs.append(_('记录%(idx)d —— 标题：%(title)s，编号：%(id)s') % {
                            'idx': idx,
                            'title': record.get('title', '?'),
                            'id': record.get('book_id', record.get('id', '?'))
                        })
                        logs.append(json.dumps(record, ensure_ascii=False, indent=2))
                    self.test_log.setPlainText('\n'.join(logs))
                except Exception as err:
                    self.test_log.setPlainText(_('测试失败：%(error)s') % {'error': err})
                finally:
                    self.test_button.setEnabled(True)

        widget = _Config(self)
        widget.__class__.__name__ = 'CalibreXshConfigWidget'
        return widget

    def save_settings(self, config_widget):
        config_widget.save_settings()
        self._init_client_from_prefs()

    def _load_qt_widgets(self):
        module_candidates = (
            'calibre.gui2.qt',
            'qt.core',
            'qt.gui',
            'qt',
            'PyQt6.QtWidgets',
            'PyQt5.QtWidgets',
        )
        for name in module_candidates:
            try:
                module = import_module(name)
            except ModuleNotFoundError:
                continue
            if hasattr(module, 'QWidget') and hasattr(module, 'QFormLayout'):
                return module
        raise ModuleNotFoundError('Qt widget module not found for Calibre-XSH plugin')

    def download_cover(self, log, result_queue, abort, title=None, authors=None,
                       identifiers=None, timeout=DEFAULT_TIMEOUT, get_best_cover=False):
        identifiers = identifiers or {}
        timeout = timeout or self.timeout
        book_id = self._get_identifier(identifiers)

        cover_urls = []
        cached = self.get_cached_cover_url(identifiers)
        if cached:
            cover_urls.append(cached)

        if book_id and not abort.is_set():
            record = self._load_by_id(book_id, log, fields='cover')
            direct_cover = (record or {}).get('cover_url')
            if direct_cover:
                cover_urls.append(direct_cover)
                try:
                    self.cache_identifier_to_cover_url(book_id, direct_cover)
                except Exception:
                    log.exception('Calibre-XSH: 缓存封面URL失败:')

        if not cover_urls and not abort.is_set():
            tmp_queue = Queue()
            self.identify(log, tmp_queue, abort, title=title, authors=authors, identifiers=identifiers)
            while not tmp_queue.empty():
                try:
                    mi = tmp_queue.get_nowait()
                except Empty:
                    break
                candidate = self.get_cached_cover_url(mi.identifiers)
                if not candidate:
                    candidate = getattr(mi, 'cover', None)
                if candidate:
                    cover_urls.append(candidate)

        seen = set()
        unique_urls = []
        for url in cover_urls:
            if url and url not in seen:
                seen.add(url)
                unique_urls.append(url)

        if not unique_urls or abort.is_set():
            log.info('Calibre-XSH: 未找到封面URL。')
            return

        br = self.browser
        for url in unique_urls:
            if abort.is_set():
                break
            try:
                br.set_current_header('User-Agent', random_user_agent())
                data = br.open_novisit(url, timeout=timeout).read()
                if data:
                    result_queue.put((self, data))
                    break
            except Exception:
                log.exception('Calibre-XSH: 下载封面失败:')

    def _get_identifier(self, identifiers):
        value = identifiers.get(PROVIDER_ID) or identifiers.get('xsh')
        if value:
            return str(value)
        return None

    def _load_by_id(self, book_id, log, fields='all'):
        if not self.catalog_client:
            log.error('Calibre-XSH: 目录客户端未初始化。')
            return None
        try:
            return self.catalog_client.get_book(book_id, fields=fields)
        except XshCatalogError as err:
            log.error(f'Calibre-XSH: 根据ID获取书籍失败({book_id}): {err}')
            return None

    def _extract_inline_title_and_authors(self, title):
        if not title:
            return title, None, False
        parts = re.split(r'\s*----\s*', title, maxsplit=1)
        if len(parts) != 2:
            return title, None, False
        inline_title, inline_authors = parts[0].strip(), parts[1].strip()
        if not inline_title or not inline_authors:
            return title, None, False
        authors = [item for item in re.split(r'[、，,/;&\s]+', inline_authors) if item]
        if not authors:
            authors = [inline_authors]
        return inline_title, authors, True

    def _build_queries(self, title, authors, identifiers, force_author=False):
        keyword = (identifiers or {}).get('isbn')
        if keyword:
            keyword = keyword.strip()
        if not keyword and title:
            keyword = self._sanitize_title(title)
        if not keyword and title:
            keyword = title.strip()
        if not keyword:
            official = (identifiers or {}).get('official')
            if official:
                keyword = official.strip()
        if not keyword:
            return []
        queries = []
        author_terms = self._filter_authors(authors)
        include_author = force_author or self.search_with_author
        if include_author and author_terms:
            queries.append(f"{keyword} {' '.join(author_terms)}")
        queries.append(keyword)
        normalized = []
        seen = set()
        for candidate in queries:
            cleaned = re.sub(r'\s+', ' ', (candidate or '').strip())
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                normalized.append(cleaned)
        return normalized

    def _emit_metadata(self, record, result_queue, log, abort):
        if abort.is_set():
            return
        mi = self._record_to_metadata(record, log)
        if mi is None:
            return
        book_id = mi.identifiers.get(PROVIDER_ID)
        if mi.cover and book_id:
            self.cache_identifier_to_cover_url(book_id, mi.cover)
        self.clean_downloaded_metadata(mi)
        result_queue.put(mi)

    def _normalize_record(self, record):
        if not isinstance(record, dict):
            return record
        data = dict(record)
        if 'author' not in data:
            author_name = data.get('author_name')
            if author_name:
                data['author'] = {'name': author_name}
        if 'categories' not in data:
            category = data.get('category')
            if category:
                data['categories'] = [category]
        if 'source_site' not in data or not data.get('source_site'):
            data['source_site'] = 'XiaoShuoHub'
        return data

    def _record_to_metadata(self, record, log):
        if not record:
            return None
        title = record.get('title') or record.get('name')
        if not title:
            log.info('Calibre-XSH: 记录缺少标题，忽略。')
            return None
        authors = self._extract_authors(record)
        mi = Metadata(title, authors if authors else [_('未知作者')])
        mi.source = PROVIDER_NAME
        mi.source_relevance = 1.0
        book_id = record.get('book_id') or record.get('id')
        identifiers = {}
        if book_id is not None:
            identifiers[PROVIDER_ID] = str(book_id)
        source_book_id = record.get('source_book_id')
        if source_book_id:
            identifiers['source_book_id'] = str(source_book_id)
        official_url = record.get('official_url')
        if official_url:
            identifiers['official'] = official_url
        mi.identifiers = identifiers or {PROVIDER_ID: title}
        mi.cover = record.get('cover_url')
        publisher = record.get('source_site') or 'XiaoShuoHub'
        mi.publisher = publisher
        intro = record.get('introduction')
        if intro:
            mi.comments = intro.strip()
        tags = self._collect_tags(record)
        if tags:
            mi.tags = tags
        rating = record.get('average_rating')
        if rating is not None:
            try:
                mi.rating = float(rating)
            except (TypeError, ValueError):
                pass
        self._annotate_status(mi, record)
        pubdate = self._parse_datetime(record.get('created_at')) or self._parse_datetime(record.get('info_updated_at'))
        if pubdate:
            mi.pubdate = pubdate
        mi.language = 'zh_CN'
        return mi

    def _extract_authors(self, record):
        author = record.get('author') or {}
        if isinstance(author, dict) and author.get('name'):
            return [author['name']]
        author_name = record.get('author_name')
        if author_name:
            return [author_name]
        return []

    def _collect_tags(self, record):
        tags = set()
        for item in record.get('categories') or []:
            name = item.get('name')
            if name:
                tags.add(name)
        for item in record.get('tags') or []:
            name = item.get('name')
            if name:
                tags.add(name)
        return sorted(tags)

    def _annotate_status(self, mi, record):
        status = record.get('status')
        if not status:
            return
        paragraph = f'<p><strong>{_("连载状态")}</strong>：{status}</p>'
        if mi.comments:
            if paragraph not in mi.comments:
                mi.comments = mi.comments.rstrip() + '\n' + paragraph
        else:
            mi.comments = paragraph

    def _parse_datetime(self, value):
        if not value:
            return None
        value = value.strip()
        variants = (
            ('%Y-%m-%dT%H:%M:%S.%f%z', True),
            ('%Y-%m-%dT%H:%M:%S%z', True),
            ('%Y-%m-%dT%H:%M:%S', False),
            ('%Y-%m-%d', False),
        )
        for fmt, accept_z in variants:
            candidate = value
            if accept_z and candidate.endswith('Z'):
                candidate = candidate[:-1] + '+0000'
            try:
                return datetime.strptime(candidate, fmt)
            except ValueError:
                continue
        return None

    def _sanitize_title(self, title):
        if not title:
            return ''
        text = title.strip()
        text = re.split(r'\s*----\s*', text, maxsplit=1)[0]
        text = re.sub(r'作者[:：].*$', '', text)
        text = text.replace('《', ' ').replace('》', ' ')
        text = re.sub(r'（[^）]*）', ' ', text)
        text = re.sub(r'\([^)]*\)', ' ', text)
        text = re.sub(r'\[[^\]]*\]', ' ', text)
        text = re.sub(r'【[^】]*】', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _filter_authors(self, authors):
        cleaned = []
        if not authors:
            return cleaned
        for author in authors:
            if not author:
                continue
            text = author.strip()
            if not text:
                continue
            lowered = text.lower()
            if text in PLACEHOLDER_AUTHORS or lowered in PLACEHOLDER_AUTHORS:
                continue
            cleaned.append(text)
        return cleaned


if __name__ == '__main__':
    from calibre.ebooks.metadata.sources.test import test_identify_plugin

    test_identify_plugin(
        CalibreXsh.name, []
    )
