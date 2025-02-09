import os
import aiohttp
import asyncio
import time
from collections import defaultdict
import re

# 配置
CONFIG = {
    "timeout": 10,  # Timeout in seconds
    "max_parallel": 30,  # Max concurrent requests
    "output_file": "best_sorted.m3u",  # Output file for the sorted M3U
    "iptv_directory": "IPTV"  # Directory containing IPTV files
}


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


# 读取 IPTV 目录下所有省份频道文件
def load_province_channels(directory="IPTV"):
    """加载所有省份的频道列表"""
    province_channels = defaultdict(set)

    for filename in os.listdir(directory):
        if filename.endswith(".txt") and filename != "CCTV.txt":  # 排除 CCTV.txt 文件
            province_name = filename.replace(".txt", "")  # 使用文件名作为省份名称
            file_path = os.path.join(directory, filename)

            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    for line in file:
                        line = line.strip()
                        if line:  # Ignore empty lines
                            province_channels[province_name].add(line)
            except FileNotFoundError:
                print(f"Error: The file {file_path} was not found.")

    return province_channels


# 正规化 CCTV 频道名称
def normalize_cctv_name(channel_name):
    """将 CCTV 频道名称进行正规化，例如 CCTV-1 -> CCTV1"""
    return re.sub(r'CCTV[-]?(\d+)', r'CCTV\1', channel_name)


# 从 TXT 文件中提取 IPTV 链接
def extract_urls_from_txt(content):
    """从 TXT 文件中提取 IPTV 链接"""
    urls = []
    for line in content.splitlines():
        line = line.strip()
        if line and ',' in line:  # 格式应该是: <频道名>,<URL>
            parts = line.split(',', 1)
            urls.append(parts)  # 提取频道名和 URL
    return urls


# 从 M3U 文件中提取 IPTV 链接
def extract_urls_from_m3u(content):
    """从 M3U 文件中提取 IPTV 链接"""
    urls = []
    lines = content.splitlines()
    channel = "Unknown"

    for line in lines:
        line = line.strip()
        if line.startswith("#EXTINF:"):
            # 从 EXTINF 标签中提取频道名
            parts = line.split(',', 1)
            channel = parts[1] if len(parts) > 1 else "Unknown"
        elif line.startswith(('http://', 'https://')):
            urls.append((channel, line))  # 存储频道和 URL 的元组
    return urls


# 测试 IPTV 链接的可用性和速度
async def test_stream(url):
    """测试 IPTV 链接的可用性和速度"""
    async with aiohttp.ClientSession(cookie_jar=None) as session:  # 禁用 cookie 处理
        start_time = time.time()
        try:
            async with session.get(url, timeout=CONFIG["timeout"]) as response:
                if response.status == 200:
                    # 计算响应时间
                    elapsed_time = time.time() - start_time
                    return True, elapsed_time
                else:
                    return False, None
        except asyncio.TimeoutError:
            return False, None
        except Exception as e:
            return False, None


# 测试多个 IPTV 链接
async def test_multiple_streams(urls):
    """测试多个 IPTV 链接"""
    tasks = [test_stream(url) for _, url in urls]
    results = await asyncio.gather(*tasks)
    return results


# 读取文件并提取 URL（支持 M3U 或 TXT 格式）
async def read_and_test_file(file_path, is_m3u=False):
    """读取文件并提取 URL 进行测试"""
    try:
        # 获取文件内容
        async with aiohttp.ClientSession(cookie_jar=None) as session:  # 禁用 cookie 处理
            async with session.get(file_path) as response:
                content = await response.text()

        # 提取 URL
        if is_m3u:
            entries = extract_urls_from_m3u(content)
        else:
            entries = extract_urls_from_txt(content)

        # 测试 URL 的可用性
        valid_urls = []
        results = await test_multiple_streams(entries)
        for (is_valid, _), (channel, url) in zip(results, entries):
            if is_valid:
                valid_urls.append((channel, url))

        return valid_urls

    except Exception as e:
        return []


# 生成排序后的 M3U 文件
def generate_sorted_m3u(valid_urls, cctv_channels, province_channels, filename):
    """生成排序后的 M3U 文件"""
    cctv_channels_list = []
    province_channels_list = defaultdict(list)
    satellite_channels = []
    other_channels = []

    for channel, url in valid_urls:
        normalized_channel = normalize_cctv_name(channel)

        if normalized_channel in cctv_channels:
            cctv_channels_list.append({
                "channel": channel,
                "url": url,
                "logo": f"https://live.fanmingming.cn/tv/{channel}.png",
                "group_title": "央视频道"
            })
        elif "卫视" in channel:  # 卫视频道
            satellite_channels.append({
                "channel": channel,
                "url": url,
                "logo": f"https://live.fanmingming.cn/tv/{channel}.png",
                "group_title": "卫视频道"
            })
        elif any(province in channel for province in province_channels):  # 本地省份频道
            for province, channels in province_channels.items():
                if channel in channels:
                    province_channels_list[province].append({
                        "channel": channel,
                        "url": url,
                        "logo": f"https://live.fanmingming.cn/tv/{channel}.png",
                        "group_title": f"{province}"
                    })
        else:
            other_channels.append({
                "channel": channel,
                "url": url,
                "logo": f"https://live.fanmingming.cn/tv/{channel}.png",
                "group_title": "其他频道"
            })

    # 排序：省份频道、卫视频道、其他频道
    for province in province_channels_list:
        province_channels_list[province].sort(key=lambda x: x["channel"])

    satellite_channels.sort(key=lambda x: x["channel"])
    other_channels.sort(key=lambda x: x["channel"])

    # 合并所有频道：CCTV -> 卫视频道 -> 省份频道 -> 其他
    all_channels = cctv_channels_list + satellite_channels + \
                   [channel for province in sorted(province_channels_list) for channel in
                    province_channels_list[province]] + \
                   other_channels

    # 写入 M3U 文件
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        for channel_info in all_channels:
            f.write(
                f"#EXTINF:-1 tvg-name=\"{channel_info['channel']}\" tvg-logo=\"{channel_info['logo']}\" group-title=\"{channel_info['group_title']}\",{channel_info['channel']}\n")
            f.write(f"{channel_info['url']}\n")


# 加载省份频道列表
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

    all_valid_urls = []

    for file_url in file_urls:
        if file_url.endswith(('.m3u', '.m3u8')):
            valid_urls = await read_and_test_file(file_url, is_m3u=True)
        elif file_url.endswith('.txt'):
            valid_urls = await read_and_test_file(file_url, is_m3u=False)
        else:
            valid_urls = []

        all_valid_urls.extend(valid_urls)

    # 生成排序后的 M3U 文件
    generate_sorted_m3u(all_valid_urls, cctv_channels, province_channels, CONFIG["output_file"])
    print(f"🎉 Generated sorted M3U file: {CONFIG['output_file']}")


if __name__ == "__main__":
    # IPTV 文件 URL（您可以添加自己的文件 URL 列表）
    file_urls = [
        "https://tzdr.com/iptv.txt",
        "https://live.kilvn.com/iptv.m3u",
        "https://cdn.jsdelivr.net/gh/Guovin/iptv-api@gd/output/result.m3u",
        "https://gh-proxy.com/raw.githubusercontent.com/vbskycn/iptv/refs/heads/master/tv/iptv4.m3u",
        "http://175.178.251.183:6689/live.m3u",
        "https://m3u.ibert.me/ycl_iptv.m3u"
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
        ".github/workflows/IPTV/云南频道.txt",
        ".github/workflows/IPTV/浙江频道.txt",
        ".github/workflows/IPTV/北京频道.txt"
    ]

    # 执行主函数
    asyncio.run(main(file_urls, cctv_channel_file, province_channel_files))
