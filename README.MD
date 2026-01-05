# Nautilus Video Duration (GStreamer) —— “时长”列扩展

一个用于 **GNOME Files (Nautilus 4 / libnautilus-extension-4)** 的扩展，  
在列表视图中新增一列 **“时长”**，显示本地视频文件的播放时长。

本项目使用 **GStreamer Discoverer** 获取媒体时长（不依赖 ffprobe），并提供：
- ✅ 线程池并行计算（线程数 = CPU 核心数）
- ✅ Discoverer 并发限制（避免大目录时 CPU/IO 爆炸）
- ✅ 缓存 + LRU 淘汰（默认 4096 项）
- ✅ 主线程安全更新（避免 Nautilus 崩溃）
- ✅ 安装到 `/usr/lib/nautilus/extensions-4`

---

## 预览

在 Nautilus 列表视图中显示新增列：

- 列名：**时长**
- 格式：`H:MM:SS`（固定宽度，便于排序）
- 处理中显示：`…`
- 失败显示：`-`

---

## 依赖

### Arch Linux（推荐安装）
基础依赖：
```bash
sudo pacman -S --needed \
  nautilus pkgconf gcc cmake ninja glib2 \
  gstreamer gst-plugins-base gst-plugins-good
