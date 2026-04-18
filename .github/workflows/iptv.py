import os
import aiohttp
import asyncio
import time
import json
from collections import Counter, defaultdict
import re
from typing import Dict, Iterable, List, Optional, Set, Tuple, Any

def contains_date(text):
    """
    检测字符串中是否包含日期格式（如 YYYY-MM-DD）
    """
    date_pattern = r"\d{4}-\d{2}-\d{2}"  # 正则表达式匹配 YYYY-MM-DD
    return re.search(date_pattern, text) is not None


def normalize_text_for_match(text: str) -> str:
    """归一化文本用于频道匹配，去掉空格/标点并统一大小写。"""
    normalized = text.translate(CHAR_NORMALIZATION_MAP).strip().upper().replace("＋", "+")
    normalized = re.sub(r"[ \t\r\n\-_|·•:：,，.。/\\()\[\]【】「」'\"`]+", "", normalized)
    return normalized


# 配置
CONFIG = {
    "timeout": 10,  # Timeout in seconds
    "max_parallel": 30,  # Max concurrent requests
    "output_file": "best_sorted.m3u",  # Output file for the sorted M3U
}

CHAR_NORMALIZATION_MAP = str.maketrans({
    "頻": "频",
    "視": "视",
    "臺": "台",
    "綜": "综",
    "聞": "闻",
    "體": "体",
    "藝": "艺",
    "經": "经",
    "濟": "济",
    "娛": "娱",
    "樂": "乐",
    "電": "电",
    "廣": "广",
    "畫": "画",
    "劇": "剧",
    "紀": "纪",
    "錄": "录",
    "網": "网",
    "導": "导",
    "髮": "发",
    "衛": "卫",
    "陰": "阴",
    "陽": "阳",
    "麗": "丽",
    "龍": "龙",
    "鄉": "乡",
    "鎮": "镇",
    "區": "区",
    "縣": "县",
    "灣": "湾",
    "滬": "沪",
    "閩": "闽",
    "贛": "赣",
    "蘇": "苏",
    "浙": "浙",
    "魯": "鲁",
    "豫": "豫",
    "鄂": "鄂",
    "湘": "湘",
    "粵": "粤",
    "瓊": "琼",
    "渝": "渝",
    "遼": "辽",
    "寧": "宁",
    "貴": "贵",
    "雲": "云",
    "藏": "藏",
    "陝": "陕",
    "晉": "晋",
    "冀": "冀",
    "贛": "赣",
    "錫": "锡",
})


PROVINCE_ALIASES = {
    "北京": {"北京台"},
    "上海": {"上海台", "东方明珠", "沪上"},
    "天津": {"天津台"},
    "重庆": {"重庆台"},
    "河北": {"河北台"},
    "山西": {"山西台", "三晋"},
    "辽宁": {"辽宁台", "辽沈"},
    "吉林": {"吉林台"},
    "内蒙": {"内蒙古"},
    "黑龙江": {"龙江", "黑龙江台"},
    "江苏": {"江苏台", "苏南"},
    "浙江": {"浙江台", "之江"},
    "安徽": {"安徽台"},
    "福建": {"福建台", "八闽"},
    "江西": {"江西台"},
    "山东": {"山东台", "齐鲁"},
    "河南": {"河南台", "中原"},
    "湖北": {"湖北台"},
    "湖南": {"湖南台"},
    "广东": {"广东台", "南粤"},
    "广西": {"广西台"},
    "海南": {"海南台"},
    "四川": {"四川台", "巴蜀"},
    "贵州": {"贵州台"},
    "云南": {"云南台", "七彩云南"},
    "西藏": {"西藏台"},
    "陕西": {"陕西台", "三秦"},
    "甘肃": {"甘肃台", "陇原"},
    "青海": {"青海台"},
    "宁夏": {"宁夏台"},
    "新疆": {"新疆台"},
}

COMMON_CHANNEL_SUFFIXES = (
    "新闻综合频道", "新闻综合", "新闻频道",
    "新聞綜合頻道", "新聞綜合", "新聞頻道",
    "社会民生频道", "社会民生",
    "社會民生頻道", "社會民生",
    "影视娱乐频道", "影视娱乐",
    "影視娛樂頻道", "影視娛樂",
    "经济生活频道", "经济生活",
    "經濟生活頻道", "經濟生活",
    "文体旅游频道", "文体旅游",
    "文體旅遊頻道", "文體旅遊",
    "文旅频道", "文旅",
    "文旅頻道",
    "旅游频道", "旅游",
    "旅遊頻道", "旅遊",
    "体育频道", "体育",
    "體育頻道", "體育",
    "教育频道", "教育",
    "教育頻道",
    "少儿频道", "少儿",
    "少兒頻道", "少兒",
    "科教频道", "科教",
    "科教頻道",
    "文化影视", "文化娱乐", "文化生活", "文化频道", "文化",
    "文化影視", "文化娛樂", "文化頻道",
    "都市频道", "都市",
    "都市頻道",
    "民生频道", "民生",
    "民生頻道",
    "资讯频道", "资讯",
    "資訊頻道", "資訊",
    "公共频道", "公共",
    "公共頻道",
    "综合频道", "综合",
    "綜合頻道",
    "娱乐频道", "娱乐",
    "娛樂頻道",
    "影视", "影視", "导视频道", "导视", "導視頻道", "導視",
    "生活频道", "生活頻道", "文艺频道", "文艺", "文藝頻道", "文藝",
    "法治频道", "法治頻道", "军事频道", "軍事頻道",
    "电视台", "電視台", "频道", "頻道", "直播",
    "高清", "超清", "标清",
)

NON_GEO_TOKENS = {
    "新闻", "综合", "公共", "生活", "民生", "都市", "经济", "科教", "教育", "少儿",
    "影视", "娱乐", "体育", "文旅", "旅游", "文化", "资讯", "导视", "频道", "电视",
    "法治", "军事", "党建", "购物", "健康", "养生", "时尚", "美食", "游戏", "电竞",
    "戲曲", "戏曲", "戲劇", "戏剧", "曲艺", "紀錄", "纪录", "綜藝", "综艺",
    "台", "TV", "HD", "SD", "UHD", "FHD", "4K", "8K",
}

# 【修改点 3】扩充智能分类关键字，防止动漫、港澳台频道被错误归类为省份频道
SMART_CATEGORY_KEYWORDS = {
    "港澳台频道": ("翡翠台", "明珠台", "无线新闻", "有线新闻", "HOY", "VIU", "凤凰", "寰宇", "纬来", "东森", "中天", "台视", "华视", "民视", "三立", "非凡", "年代", "TVBS", "八大"),
    "文旅频道": ("古城", "古镇", "景区", "景点", "风景", "风光", "观景", "全景", "大佛", "雪山", "公园", "湿地", "湖景", "山景", "游览", "花布"),
    "新闻频道": ("新闻", "时政", "资讯", "观察", "焦点", "头条"),
    "体育频道": ("体育", "足球", "篮球", "网球", "高尔夫", "搏击", "赛事"),
    "影视频道": ("电影", "影院", "剧场", "电视剧", "影视", "经典剧"),
    "少儿动漫": ("少儿", "卡通", "动漫", "动画", "童话", "小当家", "柯南", "哆啦A梦", "海绵宝宝"),
    "纪录人文": ("纪录", "纪实", "人文", "自然", "地理", "探索"),
    "音乐频道": ("音乐", "MV", "演唱会", "舞曲", "戏曲"),
    "广播频道": ("广播", "电台", "FM", "AM"),
    "戏曲综艺": ("戏曲", "戏剧", "曲艺", "梨园", "相声", "小品", "综艺", "文艺"),
    "法治军事": ("法治", "军事", "国防", "军旅", "警务", "普法"),
    "游戏电竞": ("游戏", "电竞", "电子竞技"),
    "生活购物": ("购物", "导购", "时尚", "美食", "健康", "养生", "家居"),
    "教育党建": ("党建", "党史", "党员", "教育", "教科", "留学", "考试"),
}

SCENIC_SINGLE_CHAR_HINTS = {"山", "湖", "河", "池", "田"}
SCENIC_EXCLUDE_HINTS = {
    "新闻", "综合", "公共", "体育", "足球", "篮球", "电影", "影视", "纪录",
    "动漫", "少儿", "音乐", "广播", "经济", "生活", "教育", "科教", "资讯",
    "法治", "军事", "购物", "党建", "游戏", "电竞"
}

# 【修改点 1】去掉 pcas-code 里的 s（街道/乡镇），只使用省、市、区级别的 pca-code.json
ONLINE_GEO_DATA_URLS = [
    "https://raw.githubusercontent.com/modood/Administrative-divisions-of-China/master/dist/pca-code.json",
    "https://fastly.jsdelivr.net/gh/modood/Administrative-divisions-of-China/dist/pca-code.json",
]

PROVINCE_SUFFIXES = (
    "特别行政区", "维吾尔自治区", "壮族自治区", "回族自治区", "自治区", "省", "市"
)

AREA_SUFFIXES = (
    "自治县", "自治州", "自治区", "特别行政区", "新区", "开发区", "高新区",
    "地区", "林区", "矿区", "县", "市", "区", "州", "盟", "旗", "镇", "乡", "街道"
)

IGNORED_GEO_NAMES = {
    "市辖区", "城区", "郊区", "新区", "开发区", "高新区", "矿区", "城区街道",
    "其他", "直辖", "省直辖县级行政区划", "自治区直辖县级行政区划",
    "市辖县", "县级市", "直辖县级", "工业园区", "示范区", "合作区", "管理区"
}

COMMON_CHANNEL_SUFFIXES_NORMALIZED = tuple(
    sorted({normalize_text_for_match(s) for s in COMMON_CHANNEL_SUFFIXES}, key=len, reverse=True)
)

NON_GEO_TOKENS_NORMALIZED = {
    normalize_text_for_match(token) for token in NON_GEO_TOKENS
}

SMART_CATEGORY_KEYWORDS_NORMALIZED = {
    category: tuple(sorted({normalize_text_for_match(k) for k in keywords}, key=len, reverse=True))
    for category, keywords in SMART_CATEGORY_KEYWORDS.items()
}

SCENIC_EXCLUDE_HINTS_NORMALIZED = {
    normalize_text_for_match(token) for token in SCENIC_EXCLUDE_HINTS
}

IGNORED_GEO_NAMES_NORMALIZED = {
    normalize_text_for_match(name) for name in IGNORED_GEO_NAMES
}

BLOCKED_M3U_KEYWORDS = (
    "更新时间", "更新時間", "维护时间", "維護時間", "维护内容", "維護内容", "维护內容",
    "公告说明", "公告說明", "公告", "说明", "說明", "支持作者", "支持打赏", "支持打賞",
    "免费订阅", "免費訂閲", "免費訂閱", "温馨提示", "溫馨提示", "建議使用", "建议使用",
    "请勿贩卖", "請勿販賣", "请勿频繁切换", "請勿頻繁切換", "个人觀看", "個人觀看", "刀刀影院"
)

BLOCKED_M3U_KEYWORDS_NORMALIZED = tuple(
    normalize_text_for_match(keyword) for keyword in BLOCKED_M3U_KEYWORDS
)

CHANNEL_NAME_MARKERS = (
    "卫视", "衛視", "频道", "頻道", "台", "TV", "CCTV", "CGTN", "CHC",
    "影视", "影視", "电影", "電影", "新闻", "新聞", "综合", "綜合", "体育", "體育",
    "少儿", "少兒", "科教", "经济", "經濟", "生活", "都市", "公共", "纪实", "紀實",
    "卡通", "动画", "動漫", "戏曲", "戲曲", "文旅", "电视剧", "電視劇"
)


# 读取 CCTV 频道列表
def load_cctv_channels(file_path=".github/workflows/IPTV/CCTV.txt"):
    """从文件加载 CCTV 频道列表"""
    cctv_channels = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if line:  # Ignore empty lines
                    cctv_channels.add(line)
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
    return cctv_channels


# 正规化 CCTV 频道名称
def normalize_cctv_name(channel_name):
    """将 CCTV 频道名称进行正规化，例如 CCTV-1 -> CCTV1"""
    return re.sub(r'(?i)CCTV[\s-]?(\d+\+?)', r'CCTV\1', channel_name).replace("＋", "+")


def is_cctv_channel(
    channel_name: str,
    normalized_channel: str,
    normalized_cctv_channels: Set[str]
) -> bool:
    """判断频道是否属于 CCTV/CGTN/CHC 等央视频道。"""
    cctv_number_match = re.search(r'(?i)CCTV[\s-]?(\d+\+?)', channel_name)
    if cctv_number_match:
        channel_id = f"CCTV{cctv_number_match.group(1).upper()}"
        if channel_id in normalized_cctv_channels:
            return True

    if normalized_channel in normalized_cctv_channels:
        return True

    # 处理 CCTV少儿/CGTN纪录/CHC电影 等存在后缀的场景
    for token in normalized_cctv_channels:
        if len(token) >= 4 and token in normalized_channel:
            return True

    return False


def resolve_province_aliases(province_name: str) -> Set[str]:
    aliases = {province_name}
    aliases.update(PROVINCE_ALIASES.get(province_name, set()))
    return aliases


def simplify_channel_name(channel_name: str) -> str:
    """移除常见分辨率/备注标记，便于提取地名与关键词。"""
    simplified = re.sub(r"[（(【\[][^\])）】]{0,24}[)）】\]]", "", channel_name)
    simplified = re.sub(r"\b(?:IPV6|HEVC|H\.?265|H\.?264|HDR|UHD|FHD|HD|SD|\d{3,4}P|4K|8K)\b", "", simplified, flags=re.IGNORECASE)
    return simplified.strip()


def strip_common_channel_suffixes(token: str) -> str:
    """去除频道通用后缀，尽量保留地名主干词。"""
    value = token
    value = re.sub(r"(?:TV|BTV|NBTV|CETV)\d+$", "", value)
    value = re.sub(r"[0-9一二三四五六七八九十]+套?$", "", value)
    value = re.sub(r"(?:IPV6|HEVC|H265|H264|HDR|UHD|FHD|HD|SD|4K|8K)$", "", value)

    changed = True
    while changed and value:
        changed = False
        for suffix in COMMON_CHANNEL_SUFFIXES_NORMALIZED:
            if value.endswith(suffix) and len(value) > len(suffix) + 1:
                value = value[:-len(suffix)]
                changed = True
                break

    return value


def extract_geo_tokens(channel_name: str, normalized_aliases: Set[str]) -> Set[str]:
    """从频道名中自动提取可用于省份匹配的地名词。"""
    tokens: Set[str] = set()
    simplified = simplify_channel_name(channel_name)

    candidates = [simplified]
    candidates.extend(part for part in re.split(r"[|｜/\\\-_·•\s]+", simplified) if part)

    for candidate in candidates:
        normalized = normalize_text_for_match(candidate)
        if not normalized:
            continue

        trimmed = normalized
        for alias in sorted(normalized_aliases, key=len, reverse=True):
            if trimmed.startswith(alias) and len(trimmed) > len(alias) + 1:
                trimmed = trimmed[len(alias):]
                break

        trimmed = strip_common_channel_suffixes(trimmed).strip()
        if 2 <= len(trimmed) <= 8 and trimmed not in NON_GEO_TOKENS_NORMALIZED:
            tokens.add(trimmed)

    return tokens


def strip_suffix_once(name: str, suffixes: Iterable[str]) -> str:
    for suffix in sorted(suffixes, key=len, reverse=True):
        if name.endswith(suffix) and len(name) > len(suffix) + 1:
            return name[:-len(suffix)]
    return name


def normalize_province_name(name: str) -> str:
    return strip_suffix_once(re.sub(r"\s+", "", name), PROVINCE_SUFFIXES)


def geo_name_variants(name: str) -> Set[str]:
    cleaned = re.sub(r"\s+", "", name)
    if not cleaned:
        return set()

    variants = {cleaned}
    stripped = strip_suffix_once(cleaned, AREA_SUFFIXES)
    if stripped and stripped != cleaned:
        variants.add(stripped)

    return {
        variant
        for variant in variants
        if len(variant) >= 2 and normalize_text_for_match(variant) not in IGNORED_GEO_NAMES_NORMALIZED
    }


def iter_named_items(payload) -> Iterable[str]:
    if isinstance(payload, list):
        for item in payload:
            yield from iter_named_items(item)
    elif isinstance(payload, dict):
        name = payload.get("name")
        if isinstance(name, str) and name.strip():
            yield name.strip()

        has_known_children = False
        for key in ("children", "cities", "districts", "items", "list", "data"):
            child = payload.get(key)
            if child is not None:
                has_known_children = True
                yield from iter_named_items(child)

        if not has_known_children and "name" not in payload:
            for key, value in payload.items():
                if isinstance(key, str) and key.strip():
                    yield key.strip()
                if isinstance(value, (list, dict)):
                    yield from iter_named_items(value)


def build_province_lookup(province_channels: Dict[str, Set[str]]) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for province_key in province_channels:
        province_base = province_key.replace("频道", "")
        candidates = set(resolve_province_aliases(province_base))
        candidates.add(normalize_province_name(province_base))
        for candidate in candidates:
            normalized = normalize_text_for_match(normalize_province_name(candidate))
            if len(normalized) >= 2 and normalized not in lookup:
                lookup[normalized] = province_key
    return lookup


def collect_online_geo_tokens(geo_payload, province_channels: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    province_lookup = build_province_lookup(province_channels)
    added_tokens: Dict[str, Set[str]] = defaultdict(set)

    if isinstance(geo_payload, list):
        province_nodes = geo_payload
    elif isinstance(geo_payload, dict):
        if isinstance(geo_payload.get("children"), list):
            province_nodes = geo_payload["children"]
        else:
            province_nodes = [
                {"name": key, "children": value}
                for key, value in geo_payload.items()
                if isinstance(value, (list, dict))
            ]
    else:
        return added_tokens

    for node in province_nodes:
        if not isinstance(node, dict):
            continue

        province_name = node.get("name")
        if not isinstance(province_name, str) or not province_name.strip():
            continue

        province_normalized = normalize_text_for_match(normalize_province_name(province_name))
        province_key = province_lookup.get(province_normalized)
        if not province_key:
            for key, matched_province in province_lookup.items():
                if key and (key in province_normalized or province_normalized in key):
                    province_key = matched_province
                    break
        if not province_key:
            continue

        for raw_name in iter_named_items(node.get("children", [])):
            for variant in geo_name_variants(raw_name):
                normalized_variant = normalize_text_for_match(variant)
                if len(normalized_variant) >= 2 and normalized_variant not in IGNORED_GEO_NAMES_NORMALIZED:
                    added_tokens[province_key].add(variant)

    return added_tokens


async def load_online_geo_tokens(
    session: aiohttp.ClientSession,
    province_channels: Dict[str, Set[str]]
) -> Dict[str, Set[str]]:
    for url in ONLINE_GEO_DATA_URLS:
        try:
            async with session.get(url, timeout=CONFIG["timeout"]) as response:
                if response.status != 200:
                    continue
                raw_text = await response.text(errors="ignore")
                payload = json.loads(raw_text)
                tokens = collect_online_geo_tokens(payload, province_channels)
                if tokens:
                    total = sum(len(items) for items in tokens.values())
                    print(f"Loaded {total} online geo tokens from: {url}")
                    return tokens
        except Exception:
            continue
    return {}


def build_province_matchers(province_channels: Dict[str, Set[str]]) -> Dict[str, List[str]]:
    """构建省份频道匹配词，优先精确词，其次省份关键词兜底。"""
    province_matchers: Dict[str, List[str]] = {}

    for province, channels in province_channels.items():
        patterns = set()
        province_base = province.replace("频道", "")
        aliases = resolve_province_aliases(province_base)
        normalized_aliases = {normalize_text_for_match(alias) for alias in aliases}

        for ch in channels:
            normalized = normalize_text_for_match(ch)
            
            # 【修改点 2】不再将整个频道的全名强行当成合法匹配词，避免脏数据污染。
            # 仅信任通过规则提取出的真正 geo_token
            for geo_token in extract_geo_tokens(ch, normalized_aliases):
                patterns.add(geo_token)

        for alias in aliases:
            normalized_alias = normalize_text_for_match(alias)
            if len(normalized_alias) >= 2:
                patterns.add(normalized_alias)

        province_matchers[province] = sorted(patterns, key=len, reverse=True)

    return province_matchers


def match_province(normalized_channel: str, province_matchers: Dict[str, List[str]]) -> Optional[str]:
    """按最长匹配词命中省份，避免短词误判。"""
    best_match_province = None
    best_score = 0

    for province, patterns in province_matchers.items():
        for pattern in patterns:
            if pattern in normalized_channel:
                score = len(pattern)
                if score > best_score:
                    best_score = score
                    best_match_province = province
                break

    return best_match_province


def match_smart_category(normalized_channel: str) -> Optional[str]:
    """为无法命中省份的频道提供主题兜底分类。"""
    for category, keywords in SMART_CATEGORY_KEYWORDS_NORMALIZED.items():
        for keyword in keywords:
            if keyword and keyword in normalized_channel:
                return category
    if any(ch in normalized_channel for ch in SCENIC_SINGLE_CHAR_HINTS):
        if not any(token in normalized_channel for token in SCENIC_EXCLUDE_HINTS_NORMALIZED):
            return "文旅频道"
    return None


def natural_sort_key(text: str) -> Tuple[Any, ...]:
    parts = re.split(r"(\d+)", text)
    key: List[Any] = []
    for part in parts:
        if part.isdigit():
            key.append(int(part))
        else:
            key.append(part.lower())
    return tuple(key)


def cctv_sort_key(channel_name: str) -> Tuple[Any, ...]:
    """
    央视频道排序：
    - CCTV 数字频道按数字升序（CCTV-1, CCTV-2 ... CCTV-16）
    - 同号里普通版优先于 + 版
    - 其余（CHC/CGTN/CCTV专题）按自然排序放后面
    """
    match = re.search(r"(?i)CCTV[\s-]?(\d+)(\+?)", channel_name)
    if match:
        num = int(match.group(1))
        is_plus = 1 if match.group(2) == "+" else 0
        return (0, num, is_plus, natural_sort_key(channel_name))
    return (1, natural_sort_key(channel_name))


def channel_identity_key(channel: str) -> str:
    """频道唯一键（用于去重与选优）。"""
    return normalize_text_for_match(normalize_cctv_name(channel))


def looks_like_notice_entry(channel: str, source_group_title: Optional[str] = None) -> bool:
    """过滤订阅提示、维护公告、更新时间等非频道条目。"""
    haystacks = [channel]
    if source_group_title:
        haystacks.append(source_group_title)

    for text in haystacks:
        raw_text = str(text or "").strip()
        if not raw_text:
            continue
        lowered = raw_text.casefold()
        if any(keyword.casefold() in lowered for keyword in BLOCKED_M3U_KEYWORDS):
            return True

        normalized = normalize_text_for_match(raw_text)
        if normalized and any(keyword in normalized for keyword in BLOCKED_M3U_KEYWORDS_NORMALIZED):
            return True
    return False


def _cleanup_extinf_payload(text: str) -> str:
    cleaned = str(text or "")
    cleaned = re.sub(r"https?://[^\s\"',]+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"""(?ix)
        \b(?:tvg-id|tvg-name|tvg-logo|group-title|catchup|catchup-source|x-tvg-url)\s*=\s*
        (?:"[^"]*"|'[^']*'|[^\s,]+)
        """,
        " ",
        cleaned,
    )
    cleaned = cleaned.replace("#EXTINF:-1", " ")
    cleaned = cleaned.replace(",", " ")
    cleaned = cleaned.replace('"', " ").replace("'", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _extract_channel_candidates(text: str) -> List[str]:
    candidates: List[str] = []
    raw_source = str(text or "")
    source = _cleanup_extinf_payload(raw_source)
    raw_without_urls = re.sub(r"https?://[^\s\"',]+", " ", raw_source, flags=re.IGNORECASE)
    if not raw_source and not source:
        return candidates

    patterns = [
        r"(?i)CCTV[\s-]?\d+\+?",
        r"(?i)(?:CGTN|CHC)[A-Z0-9+\-]*",
        r"[\u4e00-\u9fffA-Za-z0-9+]{1,24}(?:卫视|衛視|频道|頻道|影视|影視頻道|电影|電影|新闻|新聞|综合|綜合|体育|體育|少儿|少兒|科教|经济|經濟|生活|都市|公共|纪实|紀實|卡通|动画|動漫|戏曲|戲曲|文旅|电视台|電視台|电视|電視|台|TV)",
    ]

    for candidate_source in (raw_without_urls, source):
        if not candidate_source:
            continue

        for pattern in patterns:
            for match in re.findall(pattern, candidate_source):
                value = re.sub(r"\s+", "", match).strip("\"' ,")
                if value and len(value) <= 24:
                    candidates.append(value)

    token_source = _cleanup_extinf_payload(raw_without_urls)
    for token in re.split(r"[\s|/]+", token_source):
        token = token.strip("\"' ,")
        if 2 <= len(token) <= 16 and any(marker.lower() in token.lower() for marker in CHANNEL_NAME_MARKERS):
            candidates.append(token)

    return candidates


def sanitize_channel_name(channel: str, extinf_line: Optional[str] = None) -> str:
    """清洗上游频道名，修复嵌套属性导致的脏名称。"""
    raw_channel = str(channel or "").strip()
    if not raw_channel:
        return "Unknown"

    needs_repair = any(marker in raw_channel for marker in ("tvg-id=", "tvg-name=", "tvg-logo=", "group-title="))
    candidates: List[str] = []

    if needs_repair:
        candidates.extend(_extract_channel_candidates(raw_channel))
        if extinf_line:
            candidates.extend(_extract_channel_candidates(extinf_line))

    if candidates:
        scored = Counter(candidates)
        best_candidate = sorted(
            scored.items(),
            key=lambda item: (
                -item[1],
                0 if any(marker.lower() in item[0].lower() for marker in CHANNEL_NAME_MARKERS) else 1,
                len(item[0]),
            )
        )[0][0]
        return best_candidate

    cleaned = raw_channel.strip("\"' ,")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned or "Unknown"


def parse_group_title_from_extinf(extinf_line: str) -> Optional[str]:
    patterns = [
        r'group-title\s*=\s*"([^"]+)"',
        r"group-title\s*=\s*'([^']+)'",
        r"group-title\s*=\s*([^,\s]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, extinf_line, flags=re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            if value:
                return value
    return None


def infer_group_from_upstream_title(
    source_group_title: Optional[str],
    province_matchers: Dict[str, List[str]]
) -> Optional[str]:
    """将上游 group-title 作为分类先验信号。"""
    if not source_group_title:
        return None

    raw_title = source_group_title.strip()
    normalized = normalize_text_for_match(raw_title)
    if not normalized:
        return None

    if any(token in normalized for token in ("CCTV", "CGTN", "CHC")) or "央视" in raw_title:
        return "央视频道"
    if "卫视" in raw_title:
        return "卫视频道"

    province = match_province(normalized, province_matchers)
    if province:
        return province

    smart_category = match_smart_category(normalized)
    if smart_category:
        return smart_category

    return None


def deduplicate_candidate_entries(entries: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduplicated: List[Dict[str, Any]] = []
    seen: Set[Tuple[str, str]] = set()

    for entry in entries:
        channel = sanitize_channel_name(str(entry.get("channel", "")).strip())
        url = str(entry.get("url", "")).strip()
        if not channel or not url.startswith(("http://", "https://")):
            continue
        if looks_like_notice_entry(channel, entry.get("source_group_title")):
            continue

        key = (channel_identity_key(channel), url)
        if key in seen:
            continue
        seen.add(key)

        normalized_entry = dict(entry)
        normalized_entry["channel"] = channel
        normalized_entry["url"] = url
        deduplicated.append(normalized_entry)

    return deduplicated


def choose_better_entry(current_best: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    best_latency = current_best.get("latency")
    cand_latency = candidate.get("latency")
    best_score = (
        best_latency if isinstance(best_latency, (int, float)) else float("inf"),
        0 if str(current_best.get("url", "")).startswith("https://") else 1,
        len(str(current_best.get("url", ""))),
    )
    cand_score = (
        cand_latency if isinstance(cand_latency, (int, float)) else float("inf"),
        0 if str(candidate.get("url", "")).startswith("https://") else 1,
        len(str(candidate.get("url", ""))),
    )
    return candidate if cand_score < best_score else current_best


def select_best_streams(valid_entries: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    去重并选优：
    1) 同频道同 URL 去重
    2) 同频道保留最低延迟（并优先 https）的最佳 URL
    """
    best_by_channel: Dict[str, Dict[str, Any]] = {}

    for entry in valid_entries:
        channel = sanitize_channel_name(str(entry.get("channel", "")).strip())
        url = str(entry.get("url", "")).strip()
        if not channel or not url:
            continue

        key = channel_identity_key(channel)
        current = best_by_channel.get(key)
        if current is None:
            best_by_channel[key] = dict(entry)
        else:
            best_by_channel[key] = choose_better_entry(current, entry)

    selected = list(best_by_channel.values())
    selected.sort(key=lambda x: natural_sort_key(str(x.get("channel", ""))))
    return selected


# 从 TXT 文件中提取 IPTV 链接
def extract_urls_from_txt(content):
    """从 TXT 文件中提取 IPTV 链接"""
    urls: List[Dict[str, Any]] = []
    for line in content.splitlines():
        line = line.strip()
        if line and ',' in line:  # 格式应该是: <频道名>,<URL>
            channel, url = line.split(',', 1)
            channel = sanitize_channel_name(channel)
            if looks_like_notice_entry(channel):
                continue
            urls.append({
                "channel": channel,
                "url": url.strip(),
                "source_group_title": None,
            })
    return urls


# 从 M3U 文件中提取 IPTV 链接
def extract_urls_from_m3u(content):
    """从 M3U 文件中提取 IPTV 链接"""
    urls: List[Dict[str, Any]] = []
    lines = content.splitlines()
    channel = "Unknown"
    extinf_line = ""
    source_group_title = None

    for line in lines:
        line = line.strip()
        if line.startswith("#EXTINF:"):
            extinf_line = line
            # 从 EXTINF 标签中提取频道名
            parts = line.split(',', 1)
            raw_channel = parts[1] if len(parts) > 1 else "Unknown"
            channel = sanitize_channel_name(raw_channel, extinf_line)
            source_group_title = parse_group_title_from_extinf(line)
        elif line.startswith(('http://', 'https://')):
            if looks_like_notice_entry(channel, source_group_title):
                continue
            urls.append({
                "channel": channel.strip(),
                "url": line.strip(),
                "source_group_title": source_group_title,
            })
    return urls


# 测试 IPTV 链接的可用性和速度
async def test_stream(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, url: str):
    """测试 IPTV 链接的可用性和速度"""
    async with semaphore:
        start_time = time.time()
        try:
            async with session.get(url, timeout=CONFIG["timeout"]) as response:
                if response.status == 200:
                    elapsed_time = time.time() - start_time
                    return True, elapsed_time
                return False, None
        except asyncio.TimeoutError:
            return False, None
        except Exception:
            return False, None


# 测试多个 IPTV 链接
async def test_multiple_streams(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    entries: Iterable[Dict[str, Any]]
):
    """测试多个 IPTV 链接"""
    tasks = [test_stream(session, semaphore, str(entry.get("url", "")).strip()) for entry in entries]
    results = await asyncio.gather(*tasks)
    return results


# 读取文件并提取 URL（支持 M3U 或 TXT 格式）
async def read_and_test_file(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    file_path: str,
    is_m3u: bool = False
):
    """读取文件并提取 URL 进行测试"""
    try:
        async with session.get(file_path, timeout=CONFIG["timeout"]) as response:
            if response.status != 200:
                return []
            content = await response.text(errors="ignore")

        if is_m3u:
            entries = extract_urls_from_m3u(content)
        else:
            entries = extract_urls_from_txt(content)
        entries = deduplicate_candidate_entries(entries)

        valid_entries: List[Dict[str, Any]] = []
        results = await test_multiple_streams(session, semaphore, entries)
        for (is_valid, latency), entry in zip(results, entries):
            if is_valid:
                valid_entries.append({
                    "channel": entry["channel"],
                    "url": entry["url"],
                    "source_group_title": entry.get("source_group_title"),
                    "latency": latency,
                })

        return valid_entries

    except Exception:
        return []


# 生成排序后的 M3U 和 M3U8 文件
def generate_sorted_m3u(valid_entries, cctv_channels, province_channels, filename):
    """生成排序后的 M3U 和 M3U8 文件"""
    cctv_channels_list = []
    province_channels_list = defaultdict(list)
    satellite_channels = []
    smart_category_channels = defaultdict(list)
    other_channels = []
    normalized_cctv_channels = {
        normalize_text_for_match(normalize_cctv_name(name)) for name in cctv_channels
    }
    province_matchers = build_province_matchers(province_channels)

    for entry in valid_entries:
        channel = str(entry.get("channel", "")).strip()
        url = str(entry.get("url", "")).strip()
        source_group_title = entry.get("source_group_title")
        if not channel or not url:
            continue

        if contains_date(channel) or contains_date(url):
            continue  # 过滤掉包含日期格式的频道

        normalized_channel = normalize_text_for_match(normalize_cctv_name(channel))
        upstream_group = infer_group_from_upstream_title(source_group_title, province_matchers)

        # 根据频道名判断属于哪个分组
        if is_cctv_channel(channel, normalized_channel, normalized_cctv_channels) or upstream_group == "央视频道":
            cctv_channels_list.append({
                "channel": channel,
                "url": url,
                "logo": f"https://live.fanmingming.cn/tv/{channel}.png",
                "group_title": "央视频道"
            })
        elif "卫视" in channel or upstream_group == "卫视频道":  # 卫视频道
            satellite_channels.append({
                "channel": channel,
                "url": url,
                "logo": f"https://live.fanmingming.cn/tv/{channel}.png",
                "group_title": "卫视频道"
            })
        else:
            province = match_province(normalized_channel, province_matchers)
            if province:
                province_channels_list[province].append({
                    "channel": channel,
                    "url": url,
                    "logo": f"https://live.fanmingming.cn/tv/{channel}.png",
                    "group_title": f"{province}"
                })
            else:
                smart_category = upstream_group if upstream_group in SMART_CATEGORY_KEYWORDS else match_smart_category(normalized_channel)
                if smart_category and smart_category in SMART_CATEGORY_KEYWORDS:
                    smart_category_channels[smart_category].append({
                        "channel": channel,
                        "url": url,
                        "logo": f"https://live.fanmingming.cn/tv/{channel}.png",
                        "group_title": smart_category
                    })
                else:
                    other_channels.append({
                        "channel": channel,
                        "url": url,
                        "logo": f"https://live.fanmingming.cn/tv/{channel}.png",
                        "group_title": "其他频道"
                    })

    # 排序：省份频道、主题频道、卫视频道、其他频道
    cctv_channels_list.sort(key=lambda x: cctv_sort_key(x["channel"]))

    for province in province_channels_list:
        province_channels_list[province].sort(key=lambda x: natural_sort_key(x["channel"]))
    for smart_category in smart_category_channels:
        smart_category_channels[smart_category].sort(key=lambda x: natural_sort_key(x["channel"]))

    satellite_channels.sort(key=lambda x: natural_sort_key(x["channel"]))
    other_channels.sort(key=lambda x: natural_sort_key(x["channel"]))

    # 合并所有频道：CCTV -> 卫视频道 -> 省份频道 -> 主题频道 -> 其他
    all_channels = cctv_channels_list + satellite_channels + \
                   [channel for province in sorted(province_channels_list) for channel in
                    province_channels_list[province]] + \
                   [channel for smart_category in SMART_CATEGORY_KEYWORDS for channel in
                    smart_category_channels.get(smart_category, [])] + \
                   other_channels

    # 生成 m3u8 的文件名 (将后缀 .m3u 替换为 .m3u8)
    m3u8_filename = filename.replace('.m3u', '.m3u8')
    generated_at = time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime())
    
    # 写入 M3U 和 M3U8 文件
    for fname in [filename, m3u8_filename]:
        with open(fname, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            f.write(f"# Generated-Time: {generated_at}\n")
            f.write(f"# Channel-Count: {len(all_channels)}\n")
            for channel_info in all_channels:
                f.write(
                    f"#EXTINF:-1 tvg-name=\"{channel_info['channel']}\" tvg-logo=\"{channel_info['logo']}\" group-title=\"{channel_info['group_title']}\",{channel_info['channel']}\n")
                f.write(f"{channel_info['url']}\n")

def load_province_channels(files):
    """加载多个省份的频道列表"""
    province_channels = defaultdict(set)

    for file_path in files:
        province_name = os.path.basename(file_path).replace(".txt", "")  # 使用文件名作为省份名称

        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    line = line.strip()
                    if line:  # 忽略空行
                        province_channels[province_name].add(line)
        except FileNotFoundError:
            print(f"Error: The file {file_path} was not found.")

    return province_channels


# 主函数：处理多个文件并生成 M3U 输出
async def main(file_urls, cctv_channel_file, province_channel_files):
    """主函数处理多个文件"""
    # 加载 CCTV 频道列表
    cctv_channels = load_cctv_channels(cctv_channel_file)

    # 加载多个省份频道列表
    province_channels = load_province_channels(province_channel_files)

    all_valid_entries: List[Dict[str, Any]] = []
    semaphore = asyncio.Semaphore(CONFIG["max_parallel"])

    timeout = aiohttp.ClientTimeout(total=CONFIG["timeout"])
    connector = aiohttp.TCPConnector(limit=CONFIG["max_parallel"] * 2)
    async with aiohttp.ClientSession(cookie_jar=None, timeout=timeout, connector=connector) as session:
        online_geo_tokens = await load_online_geo_tokens(session, province_channels)
        if online_geo_tokens:
            for province, tokens in online_geo_tokens.items():
                province_channels[province].update(tokens)
            print("Online geo classification tokens merged.")
        else:
            print("Online geo tokens unavailable, fallback to local province txt only.")

        for file_url in file_urls:
            if file_url.endswith(('.m3u', '.m3u8')):
                valid_entries = await read_and_test_file(session, semaphore, file_url, is_m3u=True)
            elif file_url.endswith('.txt'):
                valid_entries = await read_and_test_file(session, semaphore, file_url, is_m3u=False)
            else:
                valid_entries = []

            all_valid_entries.extend(valid_entries)

    deduplicated_entries = deduplicate_candidate_entries(all_valid_entries)
    best_entries = select_best_streams(deduplicated_entries)
    print(f"Valid streams: {len(all_valid_entries)}, deduplicated: {len(deduplicated_entries)}, best-per-channel: {len(best_entries)}")

    # 生成排序后的 M3U 文件
    generate_sorted_m3u(best_entries, cctv_channels, province_channels, CONFIG["output_file"])
    print(f"Generated sorted M3U file: {CONFIG['output_file']}")


if __name__ == "__main__":
    # IPTV 文件 URL（您可以添加自己的文件 URL 列表）
    file_urls = [
        "https://tzdr.com/iptv.txt",
        "https://live.kilvn.com/iptv.m3u",
        "https://m3u.ibert.me/txt/fmml_itv.txt",
        "https://gh-proxy.com/raw.githubusercontent.com/vbskycn/iptv/refs/heads/master/tv/iptv4.m3u",
        "http://175.178.251.183:6689/live.m3u",
        "https://raw.githubusercontent.com/suxuang/myIPTV/refs/heads/main/ipv4.m3u",
        "https://m3u.ibert.me/ycl_iptv.m3u",
        "https://tv.iill.top/m3u/Gather",
        "https://live.zbds.org/tv/iptv4.m3u",
        "https://raw.githubusercontent.com/YueChan/Live/refs/heads/main/IPTV.m3u",
        "https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u",
        "https://raw.githubusercontent.com/BurningC4/Chinese-IPTV/master/TV-IPV4.m3u",
        "https://raw.githubusercontent.com/zwc456baby/iptv_alive/refs/heads/master/live.m3u",
        "https://raw.githubusercontent.com/hujingguang/ChinaIPTV/main/cnTV_AutoUpdate.m3u8"
    ]

    # CCTV 频道文件（例如 IPTV/CCTV.txt）
    cctv_channel_file = ".github/workflows/IPTV/CCTV.txt"

    # 省份频道文件列表
    province_channel_files = [
        ".github/workflows/IPTV/重庆频道.txt",
        ".github/workflows/IPTV/四川频道.txt",
        ".github/workflows/IPTV/云南频道.txt",
        ".github/workflows/IPTV/安徽频道.txt",
        ".github/workflows/IPTV/福建频道.txt",
        ".github/workflows/IPTV/甘肃频道.txt",
        ".github/workflows/IPTV/广东频道.txt",
        ".github/workflows/IPTV/广西频道.txt",
        ".github/workflows/IPTV/贵州频道.txt",
        ".github/workflows/IPTV/海南频道.txt",
        ".github/workflows/IPTV/河北频道.txt",
        ".github/workflows/IPTV/河南频道.txt",
        ".github/workflows/IPTV/黑龙江频道.txt",
        ".github/workflows/IPTV/湖北频道.txt",
        ".github/workflows/IPTV/湖南频道.txt",
        ".github/workflows/IPTV/吉林频道.txt",
        ".github/workflows/IPTV/江苏频道.txt",
        ".github/workflows/IPTV/江西频道.txt",
        ".github/workflows/IPTV/辽宁频道.txt",
        ".github/workflows/IPTV/内蒙频道.txt",
        ".github/workflows/IPTV/宁夏频道.txt",
        ".github/workflows/IPTV/青海频道.txt",
        ".github/workflows/IPTV/山东频道.txt",
        ".github/workflows/IPTV/山西频道.txt",
        ".github/workflows/IPTV/陕西频道.txt",
        ".github/workflows/IPTV/上海频道.txt",
        ".github/workflows/IPTV/天津频道.txt",
        ".github/workflows/IPTV/卫视频道.txt",
        ".github/workflows/IPTV/新疆频道.txt",
        ".github/workflows/IPTV/浙江频道.txt",
        ".github/workflows/IPTV/北京频道.txt"
    ]

    # 执行主函数
    asyncio.run(main(file_urls, cctv_channel_file, province_channel_files))
