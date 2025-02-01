#!/bin/bash

# 1. 配置 IPTV 资源地址
IPTV_SOURCES=(
    "https://live.zbds.top/tv/iptv6.m3u"  
    "https://live.zbds.top/tv/iptv4.m3u"  
    "https://live.kilvn.com/iptv.m3u"    
    "https://m3u.ibert.me/fmml_ipv6.m3u"    
    "http://wx.thego.cn/ak.m3u"    
    "http://aktv.top/live.txt"    
)
TMP_DIR="iptv_tmp"
OUTPUT_FILE="playlist.m3u"

# 2. 创建临时目录
mkdir -p "$TMP_DIR"
rm -f "$TMP_DIR"/* "$OUTPUT_FILE"

# 3. 下载 IPTV 源文件
for URL in "${IPTV_SOURCES[@]}"; do
    wget -q "$URL" -O "$TMP_DIR/source.m3u"  
    cat "$TMP_DIR/source.m3u" >> "$TMP_DIR/all_streams.m3u"
done

# 4. 测试 IPTV 直播流
VALID_STREAMS=()
while IFS= read -r LINE; do
    if [[ "$LINE" =~ ^http ]]; then
        echo "测试: $LINE"
        ffmpeg -i "$LINE" -t 5 -v error -an -f null - 2>/dev/null
        if [ $? -eq 0 ]; then
            VALID_STREAMS+=("$LINE")
        fi
    else
        VALID_STREAMS+=("$LINE")
    fi
done < "$TMP_DIR/all_streams.m3u"

# 5. 生成最终的 M3U
echo "#EXTM3U" > "$OUTPUT_FILE"
for STREAM in "${VALID_STREAMS[@]}"; do
    echo "$STREAM" >> "$OUTPUT_FILE"
done

# 6. 清理临时文件
rm -rf "$TMP_DIR"

echo "M3U 生成完成: $OUTPUT_FILE"
