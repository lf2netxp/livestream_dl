FROM python:3.13-slim-bookworm

# 1. 安裝 ffmpeg、git 和 bash
RUN apt-get update && apt-get install -y \
    ffmpeg \
    git \
    curl \
    ca-certificates \
    bash \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. 複製 requirements.txt 並安裝依賴
COPY requirements.txt .
RUN if [ -f "requirements.txt" ]; then \
        pip install --no-cache-dir -r requirements.txt; \
    else \
        echo "警告: requirements.txt 不存在，跳過 pip 安裝"; \
    fi

# 3. 複製所有程序文件（包括補丁腳本）
COPY . .

# 4. 創建必要的運行目錄
RUN mkdir -p YTliveDL TEMPdelete

# 5. 安裝 yt-dlp 和 ejs
RUN echo "========================================" && \
    echo "  同時安裝 yt-dlp 和 ejs" && \
    echo "========================================" && \
    # 使用實際的提交哈希
    YTDLP_HASH="5bf91072bcfbb26e6618d668a0b3379a3a862f8c" && \
    EJS_HASH="e91d03f58a9791da2300c5f10a2955af1c5d6d87" && \
    echo "yt-dlp 提交: $(echo $YTDLP_HASH | cut -c1-7)" && \
    echo "ejs 提交: $(echo $EJS_HASH | cut -c1-7)" && \
    echo "========================================" && \
    # 1. 安裝 yt-dlp
    echo "1. 安裝 yt-dlp..." && \
    git init yt-dlp-build && \
    cd yt-dlp-build && \
    git remote add origin https://github.com/yt-dlp/yt-dlp.git && \
    git fetch origin $YTDLP_HASH && \
    git checkout FETCH_HEAD && \
    pip install --force-reinstall . && \
    cd .. && \
    rm -rf yt-dlp-build && \
    echo "✅ yt-dlp 安裝完成！" && \
    # 2. 安裝 ejs
    echo "2. 安裝 ejs..." && \
    git init ejs-build && \
    cd ejs-build && \
    git remote add origin https://github.com/yt-dlp/ejs.git && \
    git fetch origin $EJS_HASH && \
    git checkout FETCH_HEAD && \
    python hatch_build.py && \
    pip install --force-reinstall . && \
    cd .. && \
    rm -rf ejs-build && \
    echo "✅ ejs 安裝完成！" && \
    echo "========================================" && \
    echo "全部安裝完成！"

# 6. 應用 yt-dlp 補丁
RUN echo "=============================================" && \
    echo "yt-dlp適配livestream_dl 自動補丁程序" && \
    echo "=============================================" && \
    python /app/patch_script.py auto

# 7. 最終檢查
RUN echo "========================================" && \
    echo "最終檢查..." && \
    echo "========================================" && \
    # 檢查 yt-dlp 版本
    python -c "import yt_dlp; print(f'✅ yt-dlp 版本: {yt_dlp.version.__version__}')" && \
    # 再次驗證補丁
    python /app/patch_script.py verify && \
    # 顯示安裝的套件
    pip list | grep -E "(yt-dlp|ejs)" && \
    echo "========================================" && \
    echo "構建完成！"

# 8. 清理補丁腳本（可選）
# RUN rm -f /app/patch_script.py

# 9. 設置入口
ENTRYPOINT ["python", "runner.py"]
