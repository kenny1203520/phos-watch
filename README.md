# phos-watch

**Event-driven automated media asset management for existing folder structures.**

phos-watch is a concept and roadmap for a lightweight background engine that watches local folders, processes newly added media asynchronously, normalizes file formats, and exposes a web-based admin surface for live configuration updates.

This repository is currently an early-stage skeleton. The README describes the intended product direction and the feature requirements that the implementation should satisfy.

---

## What This Project Is

phos-watch is not meant to be another photo gallery UI. The goal is to build a mountable media-processing companion that sits next to an existing file layout, monitors changes in real time, and keeps file organization consistent without forcing a manual workflow.

The intended use case is a folder-based media pipeline where users upload images through a file manager, and phos-watch automatically detects the new files, converts them, renames them consistently, removes original source files when needed, and reports progress through a browser UI.

## Project Vision

### 中文

phos-watch 的定位是「基於事件驅動的自動化媒體資產管理解決方案」。它不是另一個相簿網站，而是一個可以掛載在既有資料夾結構旁邊的背景處理引擎，搭配 Web 管理後台，負責即時監聽、排隊處理、格式轉換、檔名規格化與原檔清理。

### English

phos-watch is positioned as an event-driven automated media asset management solution. It is not another gallery front end. Instead, it is a background processing engine that lives beside an existing folder structure, provides a web admin console, and handles real-time watching, queued processing, format conversion, naming normalization, and source-file cleanup.

---

## Feature Requirements

The following checklist reflects the expected capabilities for the eventual implementation or for evaluating existing open-source alternatives.

### 1. Input and Trigger

- File system watching for local folders or Docker volumes.
- Real-time detection of file events such as close-write changes.
- Recursive watching for nested subfolders.
- Asynchronous background processing so uploads are never blocked.

### 2. Image Processing Core

- Automatic conversion to a chosen global target format such as WebP, JPEG, or PNG.
- Support for image processing backends such as ImageMagick, Libvips, or Pillow.
- Extension normalization rules for mixed-case or inconsistent suffixes.
- Ability to standardize formats such as forcing .jpeg, .JPG, and .JPEG to .jpg.
- Optional original-file cleanup after successful conversion.

### 3. Management and Networking

- Independent web-based administration UI.
- Hot-reloadable configuration updates without restarting the service.
- Real-time log streaming through WebSocket or Server-Sent Events.
- Docker-ready deployment for volume sharing and reverse-proxy integration.

### 4. Operational Expectations

- Minimal footprint suitable for background deployment.
- Easy integration with File Browser, Nginx Proxy Manager, or Cloudflare Tunnel.
- Clear visibility into conversion progress and failure states.

---

## Why This Matters

Common media tools often solve only part of the problem:

- Some tools convert files but do not delete the original upload.
- Some tools provide image libraries but do not watch a live directory tree.
- Some tools rely on periodic scans instead of event-driven file system events.
- Some tools lack hot-reloadable admin settings or live logs.

phos-watch is intended to fill that gap by focusing on folder-native automation rather than gallery-centric browsing.

---

## Target Behavior

The eventual implementation should behave like this:

1. A user adds a new image into a watched directory.
2. The watcher detects the event immediately.
3. The file is placed into a background queue.
4. The worker converts or normalizes the image according to active rules.
5. The system optionally removes the original upload after a successful conversion.
6. The admin UI reflects progress and errors in real time.
7. Configuration changes take effect without restarting the service.

---

## Suggested Architecture

The intended architecture is simple and modular:

- Watcher: monitors directory changes and emits events.
- Queue: buffers work items and prevents blocking writes.
- Worker: performs conversion, renaming, and cleanup.
- Rules engine: applies extension and format policies.
- Web admin UI: exposes settings and live status.
- Log transport: streams runtime messages to the browser.

This structure keeps the system easy to containerize and straightforward to integrate with existing file workflows.

---

## Current Status

This repository currently contains only a minimal project skeleton.

- [README.md](README.md) documents the intended requirements and direction.
- [app.py](app.py) is still empty.

There is no runnable implementation yet, so the README deliberately avoids claiming installation or startup steps that do not exist.

---

## Roadmap

### Phase 1

- Define the watcher and queue contract.
- Decide the image-processing backend.
- Implement extension normalization rules.
- Add a cleanup policy for original files.

### Phase 2

- Build the web admin interface.
- Add hot-reloadable settings.
- Stream logs and conversion events in real time.

### Phase 3

- Package the service for Docker.
- Validate recursive watching in nested folders.
- Test integration with File Browser and reverse proxies.

---

## Search Terms

If you are looking for existing open-source projects with similar goals, search for:

- event-driven image processing
- recursive file watcher
- asynchronous media conversion
- web admin for file watcher
- Docker image processing queue
- hot reload configuration UI
- real-time log streaming WebSocket SSE

---

## License

License information has not been defined yet.

---

## 中文摘要

phos-watch 的目標是成為一個事件驅動、可容器化、可熱更新設定的自動化媒體處理引擎。它會監聽資料夾變化，把新圖片丟進背景佇列處理，統一轉檔與副檔名規則，並在需要時刪除原檔，以避免檔案管理工具中出現重複影像。

## English Summary

phos-watch aims to be an event-driven, container-ready, hot-reloadable media automation engine. It watches folders for changes, pushes new images into a background queue, normalizes formats and extensions, and can remove originals after conversion to avoid duplicate files in file browsers.

---

## Redis is optional

phos-watch supports a Redis-backed queue but Redis is optional. By default the project will attempt to use the address in the `REDIS_URL` environment variable. To explicitly disable Redis and use the local file fallback behavior, set the environment variable `PHOS_USE_REDIS=0` (or `false`). Alternatively, leave `REDIS_URL` empty.

Example (PowerShell):

```powershell
$env:PHOS_USE_REDIS = '0'
.venv\Scripts\python app.py --mode worker
```

When Redis is disabled or unreachable, phos-watch uses a simple append/read `queue.log` file in the working directory as a fallback queue.
