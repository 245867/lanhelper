// MFC 局域网文件传输共享：默认共享 EXE 所在目录，端口 8000。
// 同一 192.168 网段运行本程序的设备会自动发现，并由窗口显示传输状态。
#define WIN32_LEAN_AND_MEAN
#include <afxwin.h>
#include <afxcmn.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <iphlpapi.h>
#include <shellapi.h>
#include <shlobj.h>
#include <uxtheme.h>
#include <process.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string>
#include <vector>
#include <deque>
#include <algorithm>
#include <map>
#include <string.h>
#include <stdint.h>
#pragma comment(lib, "ws2_32.lib")
#pragma comment(lib, "shell32.lib")
#pragma comment(lib, "iphlpapi.lib")
#pragma comment(lib, "uxtheme.lib")

static std::wstring g_root; // 规范化后的根目录，无尾部反斜杠
static int g_httpPort = 8000;
static const unsigned short DISCOVERY_PORT = 48765;
static std::string g_localIp;

struct Peer {
    std::string ip;
    int port;
    std::string name;
    ULONGLONG lastSeen;
};
static std::vector<Peer> g_peers;
static CRITICAL_SECTION g_peerLock;

struct Transfer {
    LONG id;
    std::string file;
    std::string client;
    ULONGLONG total;
    ULONGLONG sent;
    ULONGLONG startedAt;
    ULONGLONG updatedAt;
    bool complete;
};

struct RemoteEntry {
    std::string name;
    std::string href;
    unsigned long long size;
    bool dir;
};

struct DownloadTask {
    Peer peer;
    std::string url;
    std::wstring relativePath;
};
static std::vector<Transfer> g_transfers;
static CRITICAL_SECTION g_transferLock;
static LONG g_nextTransferId = 0;
static volatile LONG g_serverRunning = 0;
static SOCKET g_listenSocket = INVALID_SOCKET;
static std::deque<SOCKET> g_requestQueue;
static CRITICAL_SECTION g_requestQueueLock;
static CONDITION_VARIABLE g_requestAvailable;
static std::vector<HANDLE> g_requestWorkers;
static volatile LONG g_requestQueueStopping = 0;
static int g_transferWorkerCount = 0;
static std::deque<DownloadTask> g_downloadQueue;
static CRITICAL_SECTION g_downloadQueueLock;
static CONDITION_VARIABLE g_downloadAvailable;
static std::vector<HANDLE> g_downloadWorkers;
static volatile LONG g_downloadQueueStopping = 0;

// ---------- 编码工具 ----------
static std::string W2U(const std::wstring& w) {
    if (w.empty()) return "";
    int n = WideCharToMultiByte(CP_UTF8, 0, w.c_str(), (int)w.size(), NULL, 0, NULL, NULL);
    std::string s(n, 0);
    WideCharToMultiByte(CP_UTF8, 0, w.c_str(), (int)w.size(), &s[0], n, NULL, NULL);
    return s;
}
static std::wstring U2W(const std::string& s) {
    if (s.empty()) return L"";
    int n = MultiByteToWideChar(CP_UTF8, 0, s.c_str(), (int)s.size(), NULL, 0);
    std::wstring w(n, 0);
    MultiByteToWideChar(CP_UTF8, 0, s.c_str(), (int)s.size(), &w[0], n);
    return w;
}
static std::string urlDecode(const std::string& s) {
    std::string r; r.reserve(s.size());
    for (size_t i = 0; i < s.size(); i++) {
        if (s[i] == '%' && i + 2 < s.size() && isxdigit((unsigned char)s[i+1]) && isxdigit((unsigned char)s[i+2])) {
            r += (char)strtol(s.substr(i + 1, 2).c_str(), NULL, 16);
            i += 2;
        } else r += s[i];
    }
    return r;
}
static std::string queryValue(const std::string& query, const char* key) {
    std::string prefix = std::string(key) + "=";
    size_t start = 0;
    while (start < query.size()) {
        size_t end = query.find('&', start);
        std::string item = query.substr(start, end == std::string::npos ? std::string::npos : end - start);
        if (item.compare(0, prefix.size(), prefix) == 0) return urlDecode(item.substr(prefix.size()));
        if (end == std::string::npos) break;
        start = end + 1;
    }
    return "";
}
static std::string urlEncode(const std::string& u) { // 对 UTF-8 字节做百分号编码，保留 '/'
    static const char hex[] = "0123456789ABCDEF";
    std::string r; r.reserve(u.size() * 3);
    for (unsigned char c : u) {
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' || c == '/') r += c;
        else { r += '%'; r += hex[c >> 4]; r += hex[c & 15]; }
    }
    return r;
}
static std::string htmlEscape(const std::string& s) {
    std::string r; r.reserve(s.size());
    for (char c : s) {
        if (c == '<') r += "&lt;"; else if (c == '>') r += "&gt;";
        else if (c == '&') r += "&amp;"; else if (c == '"') r += "&quot;";
        else r += c;
    }
    return r;
}

static std::string numberString(unsigned long long value) {
    char buffer[32];
    _snprintf_s(buffer, _TRUNCATE, "%llu", value);
    return buffer;
}

static bool isLanIp(const std::string& ip) {
    return ip.size() > 8 && ip.compare(0, 8, "192.168.") == 0;
}

static void updatePeer(const std::string& ip, int port, const std::string& name) {
    if (!isLanIp(ip) || ip == g_localIp || port <= 0 || port > 65535) return;
    EnterCriticalSection(&g_peerLock);
    for (Peer& peer : g_peers) {
        if (peer.ip == ip && peer.port == port) {
            peer.name = name;
            peer.lastSeen = GetTickCount64();
            LeaveCriticalSection(&g_peerLock);
            return;
        }
    }
    Peer peer = { ip, port, name, GetTickCount64() };
    g_peers.push_back(peer);
    LeaveCriticalSection(&g_peerLock);
}

static std::vector<Peer> currentPeers() {
    std::vector<Peer> peers;
    ULONGLONG now = GetTickCount64();
    EnterCriticalSection(&g_peerLock);
    for (std::vector<Peer>::iterator it = g_peers.begin(); it != g_peers.end();) {
        if (now - it->lastSeen > 12000) it = g_peers.erase(it);
        else { peers.push_back(*it); ++it; }
    }
    LeaveCriticalSection(&g_peerLock);
    return peers;
}

static LONG beginTransfer(SOCKET socket, const std::wstring& path, ULONGLONG total) {
    sockaddr_in clientAddress = {};
    int clientAddressLength = sizeof(clientAddress);
    char clientIp[INET_ADDRSTRLEN] = "unknown";
    if (getpeername(socket, (sockaddr*)&clientAddress, &clientAddressLength) == 0)
        inet_ntop(AF_INET, &clientAddress.sin_addr, clientIp, sizeof(clientIp));
    size_t slash = path.find_last_of(L'\\');
    Transfer transfer = {};
    transfer.id = InterlockedIncrement(&g_nextTransferId);
    transfer.file = W2U(slash == std::wstring::npos ? path : path.substr(slash + 1));
    transfer.client = clientIp;
    transfer.total = total;
    transfer.startedAt = transfer.updatedAt = GetTickCount64();
    EnterCriticalSection(&g_transferLock);
    g_transfers.push_back(transfer);
    LeaveCriticalSection(&g_transferLock);
    return transfer.id;
}

static LONG beginDownloadTransfer(const std::wstring& path, const std::string& peer, ULONGLONG total) {
    size_t slash = path.find_last_of(L'\\');
    Transfer transfer = {};
    transfer.id = InterlockedIncrement(&g_nextTransferId);
    transfer.file = W2U(slash == std::wstring::npos ? path : path.substr(slash + 1));
    transfer.client = peer;
    transfer.total = total;
    transfer.startedAt = transfer.updatedAt = GetTickCount64();
    EnterCriticalSection(&g_transferLock);
    g_transfers.push_back(transfer);
    LeaveCriticalSection(&g_transferLock);
    return transfer.id;
}

static void updateTransfer(LONG id, ULONGLONG sent, bool complete) {
    EnterCriticalSection(&g_transferLock);
    for (Transfer& transfer : g_transfers) {
        if (transfer.id == id) {
            transfer.sent = sent;
            transfer.updatedAt = GetTickCount64();
            transfer.complete = complete;
            break;
        }
    }
    LeaveCriticalSection(&g_transferLock);
}

static std::vector<Transfer> currentTransfers() {
    std::vector<Transfer> result;
    ULONGLONG now = GetTickCount64();
    EnterCriticalSection(&g_transferLock);
    for (std::vector<Transfer>::iterator it = g_transfers.begin(); it != g_transfers.end();) {
        if (it->complete && now - it->updatedAt > 60000) it = g_transfers.erase(it);
        else { result.push_back(*it); ++it; }
    }
    LeaveCriticalSection(&g_transferLock);
    return result;
}

static std::wstring exeDirectory() {
    wchar_t path[MAX_PATH * 4];
    DWORD n = GetModuleFileNameW(NULL, path, MAX_PATH * 4);
    std::wstring value(path, n);
    size_t slash = value.find_last_of(L"\\/");
    return slash == std::wstring::npos ? L"." : value.substr(0, slash);
}

// ---------- 发送工具 ----------
static bool sendAll(SOCKET c, const char* p, size_t n) {
    while (n) {
        int k = send(c, p, (int)(n > 1024 * 1024 ? 1024 * 1024 : n), 0);
        if (k <= 0) return false;
        p += k; n -= k;
    }
    return true;
}
static void sendResp(SOCKET c, const char* status, const char* ctype, const std::string& body, const char* extra = "", bool headOnly = false) {
    char h[512];
    int hn = _snprintf_s(h, _TRUNCATE,
        "HTTP/1.1 %s\r\nContent-Type: %s\r\nContent-Length: %llu\r\nConnection: close\r\n%s\r\n",
        status, ctype, (unsigned long long)body.size(), extra);
    sendAll(c, h, hn);
    if (!headOnly) sendAll(c, body.data(), body.size());
}

// ---------- 目录列表 ----------
struct Entry { std::wstring name; bool dir; unsigned long long size; };

static std::string jsonEscape(const std::string& s) {
    std::string r;
    for (unsigned char c : s) {
        if (c == '"') r += "\\\"";
        else if (c == '\\') r += "\\\\";
        else if (c == '\n') r += "\\n";
        else if (c == '\r') r += "\\r";
        else if (c == '\t') r += "\\t";
        else if (c < 0x20) { char b[8]; _snprintf_s(b, _TRUNCATE, "\\u%04x", c); r += b; }
        else r += (char)c;
    }
    return r;
}

// Build a manifest of original files. The browser downloads each file directly,
// avoiding archive creation and compression for large folders.
static void appendManifest(const std::wstring& dir, const std::string& urlBase, const std::string& relativeBase,
                           std::string& out, bool& first) {
    WIN32_FIND_DATAW fd;
    HANDLE h = FindFirstFileW((dir + L"\\*").c_str(), &fd);
    if (h == INVALID_HANDLE_VALUE) return;
    do {
        if (!wcscmp(fd.cFileName, L".") || !wcscmp(fd.cFileName, L"..")) continue;
        std::wstring child = dir + L"\\" + fd.cFileName;
        std::string name = W2U(fd.cFileName);
        std::string childUrl = urlBase + urlEncode(name);
        std::string childRelative = relativeBase + name;
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
            appendManifest(child, childUrl + "/", childRelative + "/", out, first);
        } else {
            if (!first) out += ",";
            first = false;
            out += "{\"url\":\"" + jsonEscape(childUrl) + "\",\"name\":\"" + jsonEscape(name) +
                   "\",\"path\":\"" + jsonEscape(childRelative) + "\"}";
        }
    } while (FindNextFileW(h, &fd));
    FindClose(h);
}

static void serveManifest(SOCKET c, const std::wstring& dir, const std::string& urlPath, bool head) {
    std::string body = "[";
    bool first = true;
    appendManifest(dir, urlPath, "", body, first);
    body += "]";
    sendResp(c, "200 OK", "application/json; charset=utf-8", body,
             "Cache-Control: no-store\r\nAccess-Control-Allow-Origin: *\r\n", head);
}

static std::string directoryListJson(const std::wstring& dir, const std::string& urlPath) {
    std::vector<Entry> entries;
    WIN32_FIND_DATAW fd;
    HANDLE h = FindFirstFileW((dir + L"\\*").c_str(), &fd);
    if (h != INVALID_HANDLE_VALUE) {
        do {
            if (!wcscmp(fd.cFileName, L".") || !wcscmp(fd.cFileName, L"..")) continue;
            Entry e;
            e.name = fd.cFileName;
            e.dir = (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
            e.size = ((unsigned long long)fd.nFileSizeHigh << 32) | fd.nFileSizeLow;
            entries.push_back(e);
        } while (FindNextFileW(h, &fd));
        FindClose(h);
    }
    std::sort(entries.begin(), entries.end(), [](const Entry& a, const Entry& b) {
        if (a.dir != b.dir) return a.dir;
        return _wcsicmp(a.name.c_str(), b.name.c_str()) < 0;
    });
    std::string body = "[";
    for (size_t i = 0; i < entries.size(); ++i) {
        const Entry& e = entries[i];
        std::string name = W2U(e.name);
        std::string href = urlPath + urlEncode(name) + (e.dir ? "/" : "");
        if (i) body += ",";
        body += "{\"name\":\"" + jsonEscape(name) + "\",\"dir\":" + (e.dir ? "true" : "false") +
                ",\"size\":" + numberString(e.size) + ",\"href\":\"" + jsonEscape(href) + "\"}";
    }
    return body + "]";
}

static bool resolveShareDirectory(const std::string& requestPath, std::wstring& directory, std::string& canonicalPath) {
    canonicalPath = requestPath.empty() ? "/" : requestPath;
    if (canonicalPath[0] != '/') canonicalPath = "/" + canonicalPath;
    if (canonicalPath.back() != '/') canonicalPath += "/";
    std::wstring rel = U2W(urlDecode(canonicalPath));
    for (wchar_t& ch : rel) if (ch == L'/') ch = L'\\';
    std::wstring wanted = g_root + rel;
    wchar_t full[MAX_PATH * 4];
    DWORD count = GetFullPathNameW(wanted.c_str(), MAX_PATH * 4, full, NULL);
    if (!count || count >= MAX_PATH * 4 || _wcsnicmp(full, g_root.c_str(), g_root.size()) != 0 ||
        (full[g_root.size()] != 0 && full[g_root.size()] != L'\\')) return false;
    DWORD attributes = GetFileAttributesW(full);
    if (attributes == INVALID_FILE_ATTRIBUTES || !(attributes & FILE_ATTRIBUTE_DIRECTORY)) return false;
    directory = full;
    return true;
}

static void serveList(SOCKET c, const std::string& requestPath, bool head) {
    std::wstring directory;
    std::string canonicalPath;
    if (!resolveShareDirectory(requestPath, directory, canonicalPath)) {
        sendResp(c, "404 Not Found", "application/json; charset=utf-8", "[]", "Cache-Control: no-store\r\n", head);
        return;
    }
    std::string body = directoryListJson(directory, canonicalPath);
    sendResp(c, "200 OK", "application/json; charset=utf-8", body, "Cache-Control: no-store\r\n", head);
}

static bool fetchPeerPath(const Peer& peer, const std::string& requestPath, std::string& body) {
    SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) return false;
    DWORD timeout = 1200;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (char*)&timeout, sizeof(timeout));
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_port = htons((u_short)peer.port);
    if (inet_pton(AF_INET, peer.ip.c_str(), &address.sin_addr) != 1 ||
        connect(s, (sockaddr*)&address, sizeof(address)) != 0) {
        closesocket(s);
        return false;
    }
    std::string request = "GET " + requestPath + " HTTP/1.1\r\nHost: " + peer.ip + "\r\nConnection: close\r\n\r\n";
    if (!sendAll(s, request.data(), request.size())) { closesocket(s); return false; }
    std::string response;
    char buffer[4096];
    int n;
    while ((n = recv(s, buffer, sizeof(buffer), 0)) > 0 && response.size() < 1024 * 1024)
        response.append(buffer, n);
    closesocket(s);
    size_t bodyAt = response.find("\r\n\r\n");
    if (response.compare(0, 12, "HTTP/1.1 200") != 0 || bodyAt == std::string::npos) return false;
    body = response.substr(bodyAt + 4);
    return !body.empty();
}

static bool fetchPeerList(const Peer& peer, const std::string& path, std::string& list) {
    std::string request = "/api/list?path=" + urlEncode(path);
    if (!fetchPeerPath(peer, request, list)) return false;
    return list[0] == '[';
}

static bool fetchPeerList(const Peer& peer, std::string& list) {
    return fetchPeerList(peer, "/", list);
}

static bool jsonStringField(const std::string& object, const char* key, std::string& value) {
    std::string marker = std::string("\"") + key + "\":\"";
    size_t pos = object.find(marker);
    if (pos == std::string::npos) return false;
    pos += marker.size();
    value.clear();
    bool escaped = false;
    for (; pos < object.size(); ++pos) {
        char ch = object[pos];
        if (escaped) {
            if (ch == 'n') value += '\n';
            else if (ch == 'r') value += '\r';
            else if (ch == 't') value += '\t';
            else value += ch;
            escaped = false;
        } else if (ch == '\\') escaped = true;
        else if (ch == '"') return true;
        else value += ch;
    }
    return false;
}

static std::vector<std::string> jsonObjects(const std::string& json) {
    std::vector<std::string> objects;
    size_t start = std::string::npos;
    int depth = 0;
    bool inString = false, escaped = false;
    for (size_t i = 0; i < json.size(); ++i) {
        char ch = json[i];
        if (inString) {
            if (escaped) escaped = false;
            else if (ch == '\\') escaped = true;
            else if (ch == '"') inString = false;
            continue;
        }
        if (ch == '"') inString = true;
        else if (ch == '{') { if (depth++ == 0) start = i; }
        else if (ch == '}' && --depth == 0 && start != std::string::npos) {
            objects.push_back(json.substr(start, i - start + 1));
            start = std::string::npos;
        }
    }
    return objects;
}

static std::vector<RemoteEntry> parseRemoteEntries(const std::string& json) {
    std::vector<RemoteEntry> entries;
    for (const std::string& object : jsonObjects(json)) {
        RemoteEntry entry = {};
        std::string size;
        if (!jsonStringField(object, "name", entry.name) || !jsonStringField(object, "href", entry.href)) continue;
        entry.dir = object.find("\"dir\":true") != std::string::npos;
        size_t sizeAt = object.find("\"size\":");
        if (sizeAt != std::string::npos) entry.size = _strtoui64(object.c_str() + sizeAt + 7, NULL, 10);
        entries.push_back(entry);
    }
    return entries;
}

static std::vector<DownloadTask> parseManifest(const Peer& peer, const std::string& json, const std::wstring& folderName) {
    std::vector<DownloadTask> tasks;
    for (const std::string& object : jsonObjects(json)) {
        std::string url, path;
        if (!jsonStringField(object, "url", url) || !jsonStringField(object, "path", path)) continue;
        DownloadTask task = {};
        task.peer = peer;
        task.url = url;
        task.relativePath = folderName + L"\\" + U2W(path);
        tasks.push_back(task);
    }
    return tasks;
}

static bool downloadTask(const DownloadTask& task) {
    SOCKET socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (socket == INVALID_SOCKET) return false;
    DWORD timeout = 30000;
    int receiveBuffer = 4 * 1024 * 1024;
    setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));
    setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, (char*)&timeout, sizeof(timeout));
    setsockopt(socket, SOL_SOCKET, SO_RCVBUF, (char*)&receiveBuffer, sizeof(receiveBuffer));
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_port = htons((u_short)task.peer.port);
    if (inet_pton(AF_INET, task.peer.ip.c_str(), &address.sin_addr) != 1 ||
        connect(socket, (sockaddr*)&address, sizeof(address)) != 0) {
        closesocket(socket);
        return false;
    }
    std::string request = "GET " + task.url + " HTTP/1.1\r\nHost: " + task.peer.ip + "\r\nConnection: close\r\n\r\n";
    if (!sendAll(socket, request.data(), request.size())) { closesocket(socket); return false; }

    std::string response;
    char buffer[65536];
    int received = 0;
    while (response.find("\r\n\r\n") == std::string::npos && response.size() < 32768 &&
           (received = recv(socket, buffer, sizeof(buffer), 0)) > 0) response.append(buffer, received);
    size_t headerEnd = response.find("\r\n\r\n");
    if (response.compare(0, 12, "HTTP/1.1 200") != 0 || headerEnd == std::string::npos) {
        closesocket(socket);
        return false;
    }
    ULONGLONG total = 0;
    size_t contentLength = response.find("Content-Length:");
    if (contentLength != std::string::npos) total = _strtoui64(response.c_str() + contentLength + 15, NULL, 10);

    std::wstring relative = task.relativePath;
    for (wchar_t& ch : relative) if (ch == L'/') ch = L'\\';
    if (relative.empty() || relative.find(L"..") != std::wstring::npos) { closesocket(socket); return false; }
    std::wstring destination = exeDirectory() + L"\\downloads\\" + U2W(task.peer.ip) + L"\\" + relative;
    size_t slash = destination.find_last_of(L'\\');
    if (slash != std::wstring::npos) SHCreateDirectoryExW(NULL, destination.substr(0, slash).c_str(), NULL);
    HANDLE file = CreateFileW(destination.c_str(), GENERIC_WRITE, FILE_SHARE_READ, NULL, CREATE_ALWAYS,
                              FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    if (file == INVALID_HANDLE_VALUE) { closesocket(socket); return false; }

    LONG transferId = beginDownloadTransfer(destination, task.peer.ip, total);
    ULONGLONG written = 0;
    bool ok = true;
    std::string initial = response.substr(headerEnd + 4);
    if (!initial.empty()) {
        DWORD out = 0;
        ok = WriteFile(file, initial.data(), (DWORD)initial.size(), &out, NULL) && out == initial.size();
        written += out;
        updateTransfer(transferId, written, false);
    }
    while (ok && (received = recv(socket, buffer, sizeof(buffer), 0)) > 0) {
        DWORD out = 0;
        ok = WriteFile(file, buffer, received, &out, NULL) && out == (DWORD)received;
        written += out;
        updateTransfer(transferId, written, false);
    }
    CloseHandle(file);
    closesocket(socket);
    bool complete = ok && (total == 0 || written == total);
    updateTransfer(transferId, written, complete);
    return complete;
}

static unsigned __stdcall downloadPoolWorker(void*) {
    for (;;) {
        DownloadTask task = {};
        bool hasTask = false;
        EnterCriticalSection(&g_downloadQueueLock);
        while (g_downloadQueue.empty() && !InterlockedCompareExchange(&g_downloadQueueStopping, 0, 0))
            SleepConditionVariableCS(&g_downloadAvailable, &g_downloadQueueLock, INFINITE);
        if (!g_downloadQueue.empty()) {
            task = g_downloadQueue.front();
            g_downloadQueue.pop_front();
            hasTask = true;
        } else if (InterlockedCompareExchange(&g_downloadQueueStopping, 0, 0)) {
            LeaveCriticalSection(&g_downloadQueueLock);
            break;
        }
        LeaveCriticalSection(&g_downloadQueueLock);
        if (hasTask) downloadTask(task);
    }
    return 0;
}

static void startDownloadPool() {
    InterlockedExchange(&g_downloadQueueStopping, 0);
    for (int i = 0; i < 8; ++i) {
        HANDLE thread = (HANDLE)_beginthreadex(NULL, 0, downloadPoolWorker, NULL, 0, NULL);
        if (!thread) break;
        g_downloadWorkers.push_back(thread);
    }
}

static void queueDownloads(const std::vector<DownloadTask>& tasks) {
    if (tasks.empty()) return;
    EnterCriticalSection(&g_downloadQueueLock);
    for (const DownloadTask& task : tasks) g_downloadQueue.push_back(task);
    WakeAllConditionVariable(&g_downloadAvailable);
    LeaveCriticalSection(&g_downloadQueueLock);
}

static void stopDownloadPool() {
    EnterCriticalSection(&g_downloadQueueLock);
    InterlockedExchange(&g_downloadQueueStopping, 1);
    g_downloadQueue.clear();
    WakeAllConditionVariable(&g_downloadAvailable);
    LeaveCriticalSection(&g_downloadQueueLock);
    for (HANDLE thread : g_downloadWorkers) {
        WaitForSingleObject(thread, 2000);
        CloseHandle(thread);
    }
    g_downloadWorkers.clear();
}

static void servePeers(SOCKET c, bool head) {
    std::vector<Peer> peers = currentPeers();
    std::string body = "[";
    bool first = true;
    for (const Peer& peer : peers) {
        std::string files;
        if (!fetchPeerList(peer, files)) continue;
        if (!first) body += ",";
        first = false;
        body += "{\"ip\":\"" + jsonEscape(peer.ip) + "\",\"port\":" + numberString((unsigned long long)peer.port) +
                ",\"name\":\"" + jsonEscape(peer.name) + "\",\"files\":" + files + "}";
    }
    body += "]";
    sendResp(c, "200 OK", "application/json; charset=utf-8", body, "Cache-Control: no-store\r\n", head);
}

static std::string sizeStr(unsigned long long n) {
    char b[32];
    if (n < 1024) _snprintf_s(b, _TRUNCATE, "%llu B", n);
    else if (n < 1024ull * 1024) _snprintf_s(b, _TRUNCATE, "%.1f KB", n / 1024.0);
    else if (n < 1024ull * 1024 * 1024) _snprintf_s(b, _TRUNCATE, "%.1f MB", n / 1048576.0);
    else _snprintf_s(b, _TRUNCATE, "%.2f GB", n / 1073741824.0);
    return b;
}

static void serveDir(SOCKET c, const std::wstring& dir, const std::string& urlPath, bool head) {
    std::vector<Entry> es;
    WIN32_FIND_DATAW fd;
    HANDLE h = FindFirstFileW((dir + L"\\*").c_str(), &fd);
    if (h != INVALID_HANDLE_VALUE) {
        do {
            if (!wcscmp(fd.cFileName, L".") || !wcscmp(fd.cFileName, L"..")) continue;
            Entry e;
            e.name = fd.cFileName;
            e.dir = (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
            e.size = ((unsigned long long)fd.nFileSizeHigh << 32) | fd.nFileSizeLow;
            es.push_back(e);
        } while (FindNextFileW(h, &fd));
        FindClose(h);
    }
    std::sort(es.begin(), es.end(), [](const Entry& a, const Entry& b) {
        if (a.dir != b.dir) return a.dir; // 目录在前
        return _wcsicmp(a.name.c_str(), b.name.c_str()) < 0;
    });

    std::string t = htmlEscape(urlDecode(urlPath));
    std::string body;
    body.reserve(4096);
    body += "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
            "<title>" + t + "</title><style>"
            "body{font-family:Segoe UI,Microsoft YaHei,sans-serif;margin:0;background:#111827;color:#e5e7eb;min-height:100vh;display:flex;justify-content:center;align-items:flex-start;padding:40px 16px}"
            ".wrap{width:100%;max-width:980px}h3{word-break:break-all;margin:0 0 18px;color:#f9fafb}"
            "table{border-collapse:collapse;width:100%;background:#1f2937;border:1px solid #374151;border-radius:8px;overflow:hidden}"
            "td,th{padding:11px 14px;border-bottom:1px solid #374151;text-align:left}th{background:#273449;color:#cbd5e1;font-weight:600}"
            "a{text-decoration:none;color:#7dd3fc;word-break:break-all}a:hover{text-decoration:underline}"
            "td.s{white-space:nowrap;color:#9ca3af;font-size:13px}.action{white-space:nowrap;text-align:right}"
            ".btn{display:inline-block;border:1px solid #38bdf8;border-radius:5px;padding:5px 10px;color:#bae6fd;font-size:13px;cursor:pointer;background:#0f2940}"
            ".btn:hover{background:#164e63;text-decoration:none}.toolbar{margin:0 0 14px}.hint{color:#9ca3af;font-size:13px;margin:0 0 14px}"
            ".peers{margin:28px 0 0}.peers h4{margin:0 0 10px;color:#f9fafb}.peer{border:1px solid #374151;background:#1f2937;margin:8px 0;padding:12px;border-radius:7px}"
            ".peer-head{display:flex;justify-content:space-between;gap:12px;align-items:center}.peer-files{list-style:none;margin:10px 0 0;padding:0}.peer-files li{display:flex;justify-content:space-between;gap:12px;padding:6px 0;border-top:1px solid #374151}"
            ".peer-files .btn{padding:3px 8px}.empty{color:#9ca3af;font-size:13px}#downloadMenu{position:fixed;z-index:10;display:none;background:#0b1220;border:1px solid #38bdf8;padding:5px;border-radius:5px;box-shadow:0 8px 24px rgba(0,0,0,.35)}</style></head><body><div class=\"wrap\">";
    body += "<h3>" + t + "</h3><div class=\"toolbar\"><a class=\"btn\" href=\"?manifest=1\" onclick=\"downloadFolder(event, this.href)\" oncontextmenu=\"showMenu(event,this.href,true)\">下载当前文件夹</a></div><p class=\"hint\">直接传输文件夹内的每个原文件，不会压缩或打包。</p><table><tr><th>名称</th><th>大小</th><th class=\"action\">下载</th></tr>";
    if (urlPath != "/")
        body += "<tr><td><a href=\"../\">../ (上级目录)</a></td><td class=\"s\">-</td><td></td></tr>";
    for (const Entry& e : es) {
        std::string n8 = W2U(e.name);
        std::string href = urlEncode(n8) + (e.dir ? "/" : "");
        std::string downloadHref = href + (e.dir ? "?manifest=1" : "");
        body += "<tr><td><a href=\"" + href + "\">" + htmlEscape(n8) + (e.dir ? "/" : "") + "</a></td><td class=\"s\">"
              + (e.dir ? std::string("-") : sizeStr(e.size)) + "</td><td class=\"action\"><a class=\"btn\" href=\"" + downloadHref + "\""
              + (e.dir ? " onclick=\"downloadFolder(event, this.href)\" oncontextmenu=\"showMenu(event,this.href,true)\"" : " download oncontextmenu=\"showMenu(event,this.href,false)\"") + ">下载</a></td></tr>";
    }
    body += "</table><section class=\"peers\"><h4>局域网在线共享</h4><div id=\"peers\" class=\"empty\">正在查找共享节点...</div></section><div id=\"downloadMenu\"><a class=\"btn\" id=\"menuDownload\" href=\"#\">下载</a></div><script>"
            "function downloadOne(url){const a=document.createElement('a');a.href=url;a.rel='noopener';document.body.appendChild(a);a.click();a.remove()}"
            "async function downloadFolder(e,url){if(e)e.preventDefault();try{const r=await fetch(url);if(!r.ok)throw new Error();const files=await r.json();if(!files.length){alert('文件夹为空');return}files.forEach((f,i)=>setTimeout(()=>downloadOne(f.url),i*25))}catch(_){alert('无法准备文件夹下载')}}"
            "function showMenu(e,url,folder){e.preventDefault();const m=document.getElementById('downloadMenu');m.style.left=e.clientX+'px';m.style.top=e.clientY+'px';m.style.display='block';document.getElementById('menuDownload').onclick=x=>{x.preventDefault();m.style.display='none';folder?downloadFolder(null,url):downloadOne(url)};return false}document.addEventListener('click',()=>document.getElementById('downloadMenu').style.display='none');"
            "function esc(s){return String(s).replace(/[&<>]/g,c=>c==='&'?'&amp;':c==='<'?'&lt;':'&gt;').replace(/\\x22/g,'&quot;').replace(/\\x27/g,'&#39;')}"
            "async function refreshPeers(){try{const r=await fetch('/api/peers');const peers=await r.json();const box=document.getElementById('peers');if(!peers.length){box.className='empty';box.textContent='未发现其他运行中的共享节点';return}box.className='';box.innerHTML=peers.map(p=>{const base='http://'+p.ip+':'+p.port;const files=p.files.map(f=>{const url=base+f.href;const d=f.dir?url+'?manifest=1':url;return '<li><a href=\"'+esc(url)+'\" target=\"_blank\">'+esc(f.name)+(f.dir?'/':'')+'</a><button class=\"btn remote-download\" data-url=\"'+esc(d)+'\" data-folder=\"'+(f.dir?'1':'0')+'\">下载</button></li>'}).join('')||'<li class=\"empty\">空目录</li>';return '<article class=\"peer\"><div class=\"peer-head\"><a href=\"'+esc(base)+'/\" target=\"_blank\">'+esc(p.name||p.ip)+'</a><span class=\"s\">'+esc(p.ip+':'+p.port)+'</span></div><ul class=\"peer-files\">'+files+'</ul></article>'}).join('');box.querySelectorAll('.remote-download').forEach(b=>{const go=e=>b.dataset.folder==='1'?downloadFolder(e,b.dataset.url):downloadOne(b.dataset.url);b.onclick=go;b.oncontextmenu=e=>showMenu(e,b.dataset.url,b.dataset.folder==='1')})}catch(_){document.getElementById('peers').textContent='无法读取局域网共享列表'}}refreshPeers();setInterval(refreshPeers,5000);</script></div></body></html>";
    sendResp(c, "200 OK", "text/html; charset=utf-8", body, "", head);
}

// ---------- 文件下载 ----------
static void serveFile(SOCKET c, const std::wstring& path, unsigned long long size, bool head) {
    HANDLE f = CreateFileW(path.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
                           OPEN_EXISTING, FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    if (f == INVALID_HANDLE_VALUE) {
        sendResp(c, "403 Forbidden", "text/plain; charset=utf-8", "cannot open file");
        return;
    }
    size_t p = path.find_last_of(L'\\');
    std::string fn = urlEncode(W2U(p == std::wstring::npos ? path : path.substr(p + 1)));
    char h[1024];
    int hn = _snprintf_s(h, _TRUNCATE,
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: %llu\r\n"
        "Content-Disposition: attachment; filename*=UTF-8''%s\r\nConnection: close\r\n\r\n",
        size, fn.c_str());
    LONG transferId = head ? 0 : beginTransfer(c, path, size);
    ULONGLONG sent = 0;
    bool complete = false;
    if (sendAll(c, h, hn) && !head) {
        static const DWORD CHUNK = 1024 * 1024;
        std::vector<char> buf(CHUNK);
        DWORD rd;
        while (ReadFile(f, buf.data(), CHUNK, &rd, NULL) && rd > 0) {
            if (!sendAll(c, buf.data(), rd)) break;
            sent += rd;
            updateTransfer(transferId, sent, false);
        }
        complete = (sent == size);
    }
    if (transferId) updateTransfer(transferId, sent, complete);
    CloseHandle(f);
}

static unsigned __stdcall discoveryWorker(void*) {
    SOCKET s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s == INVALID_SOCKET) return 0;
    BOOL reuse = TRUE, broadcast = TRUE;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse, sizeof(reuse));
    setsockopt(s, SOL_SOCKET, SO_BROADCAST, (char*)&broadcast, sizeof(broadcast));
    DWORD timeout = 500;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));

    sockaddr_in listenAddress = {};
    listenAddress.sin_family = AF_INET;
    listenAddress.sin_addr.s_addr = INADDR_ANY;
    listenAddress.sin_port = htons(DISCOVERY_PORT);
    if (bind(s, (sockaddr*)&listenAddress, sizeof(listenAddress)) != 0) {
        closesocket(s);
        return 0;
    }

    wchar_t computerName[MAX_COMPUTERNAME_LENGTH + 1] = {};
    DWORD computerNameLen = MAX_COMPUTERNAME_LENGTH + 1;
    GetComputerNameW(computerName, &computerNameLen);
    std::string name = W2U(computerName);
    if (name.empty()) name = "LAN-SHARE";
    for (char& ch : name) if (ch == ' ') ch = '_';

    sockaddr_in broadcastAddress = {};
    broadcastAddress.sin_family = AF_INET;
    broadcastAddress.sin_addr.s_addr = INADDR_BROADCAST;
    broadcastAddress.sin_port = htons(DISCOVERY_PORT);
    ULONGLONG nextBroadcast = 0;
    while (InterlockedCompareExchange(&g_serverRunning, 0, 0)) {
        ULONGLONG now = GetTickCount64();
        if (now >= nextBroadcast) {
            std::string hello = "LANSHARE/1 DISCOVER " + numberString((unsigned long long)g_httpPort) + " " + name;
            sendto(s, hello.data(), (int)hello.size(), 0, (sockaddr*)&broadcastAddress, sizeof(broadcastAddress));
            nextBroadcast = now + 3000;
        }

        sockaddr_in from = {};
        int fromLen = sizeof(from);
        char packet[512] = {};
        int n = recvfrom(s, packet, sizeof(packet) - 1, 0, (sockaddr*)&from, &fromLen);
        if (n <= 0) continue;
        packet[n] = 0;
        char ip[INET_ADDRSTRLEN] = {};
        inet_ntop(AF_INET, &from.sin_addr, ip, sizeof(ip));
        if (!isLanIp(ip)) continue;

        int peerPort = 0;
        char peerName[256] = {};
        if (sscanf_s(packet, "LANSHARE/1 DISCOVER %d %255s", &peerPort, peerName, (unsigned)_countof(peerName)) >= 1) {
            updatePeer(ip, peerPort, peerName);
            std::string here = "LANSHARE/1 HERE " + numberString((unsigned long long)g_httpPort) + " " + name;
            sendto(s, here.data(), (int)here.size(), 0, (sockaddr*)&from, fromLen);
        } else if (sscanf_s(packet, "LANSHARE/1 HERE %d %255s", &peerPort, peerName, (unsigned)_countof(peerName)) >= 1) {
            updatePeer(ip, peerPort, peerName);
        }
    }
    closesocket(s);
    return 0;
}

// ---------- 请求处理 ----------
static unsigned __stdcall worker(void* arg) {
    SOCKET c = (SOCKET)(ULONG_PTR)arg;
    DWORD tmo = 15000;
    int sendBuffer = 4 * 1024 * 1024;
    BOOL noDelay = TRUE;
    setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, (char*)&tmo, sizeof(tmo));
    setsockopt(c, SOL_SOCKET, SO_SNDTIMEO, (char*)&tmo, sizeof(tmo));
    setsockopt(c, SOL_SOCKET, SO_SNDBUF, (char*)&sendBuffer, sizeof(sendBuffer));
    setsockopt(c, IPPROTO_TCP, TCP_NODELAY, (char*)&noDelay, sizeof(noDelay));

    std::string req;
    char buf[4096];
    while (req.find("\r\n\r\n") == std::string::npos && req.size() < 16384) {
        int k = recv(c, buf, sizeof(buf), 0);
        if (k <= 0) { closesocket(c); return 0; }
        req.append(buf, k);
    }
    size_t sp1 = req.find(' '), sp2 = (sp1 == std::string::npos) ? std::string::npos : req.find(' ', sp1 + 1);
    bool head = (req.compare(0, 5, "HEAD ") == 0);
    if (sp2 == std::string::npos || (!head && req.compare(0, 4, "GET ") != 0)) {
        sendResp(c, "400 Bad Request", "text/plain", "bad request");
        closesocket(c); return 0;
    }
    std::string rawPath = req.substr(sp1 + 1, sp2 - sp1 - 1);
    size_t q = rawPath.find('?');
    std::string query;
    if (q != std::string::npos) {
        query = rawPath.substr(q + 1);
        rawPath.resize(q);
    }
    bool manifest = (query == "manifest=1");

    if (rawPath == "/api/list") {
        serveList(c, queryValue(query, "path"), head);
        shutdown(c, SD_SEND);
        closesocket(c);
        return 0;
    }
    if (rawPath == "/api/peers") {
        servePeers(c, head);
        shutdown(c, SD_SEND);
        closesocket(c);
        return 0;
    }

    std::wstring rel = U2W(urlDecode(rawPath));
    for (wchar_t& ch : rel) if (ch == L'/') ch = L'\\';
    std::wstring want = g_root + rel;

    // 规范化并确认仍在根目录内，防止 ../ 穿越
    wchar_t full[MAX_PATH * 4];
    DWORD fn = GetFullPathNameW(want.c_str(), MAX_PATH * 4, full, NULL);
    if (fn == 0 || fn >= MAX_PATH * 4 ||
        _wcsnicmp(full, g_root.c_str(), g_root.size()) != 0 ||
        (full[g_root.size()] != 0 && full[g_root.size()] != L'\\')) {
        sendResp(c, "403 Forbidden", "text/plain", "forbidden");
        closesocket(c); return 0;
    }

    DWORD attr = GetFileAttributesW(full);
    if (attr == INVALID_FILE_ATTRIBUTES) {
        sendResp(c, "404 Not Found", "text/html; charset=utf-8", "<h3>404 未找到</h3>");
    } else if (attr & FILE_ATTRIBUTE_DIRECTORY) {
        if (rawPath.empty() || rawPath.back() != '/') { // 目录必须以 / 结尾，否则相对链接失效
            std::string loc = "Location: " + rawPath + "/\r\n";
            sendResp(c, "301 Moved Permanently", "text/plain", "", loc.c_str());
        } else if (manifest) {
            serveManifest(c, full, rawPath, head);
        } else {
            serveDir(c, full, rawPath, head);
        }
    } else {
        WIN32_FILE_ATTRIBUTE_DATA fad;
        unsigned long long size = 0;
        if (GetFileAttributesExW(full, GetFileExInfoStandard, &fad))
            size = ((unsigned long long)fad.nFileSizeHigh << 32) | fad.nFileSizeLow;
        serveFile(c, full, size, head);
    }
    shutdown(c, SD_SEND);
    closesocket(c);
    return 0;
}

static unsigned __stdcall requestPoolWorker(void*) {
    for (;;) {
        SOCKET client = INVALID_SOCKET;
        EnterCriticalSection(&g_requestQueueLock);
        while (g_requestQueue.empty() && !InterlockedCompareExchange(&g_requestQueueStopping, 0, 0))
            SleepConditionVariableCS(&g_requestAvailable, &g_requestQueueLock, INFINITE);
        if (!g_requestQueue.empty()) {
            client = g_requestQueue.front();
            g_requestQueue.pop_front();
        } else if (InterlockedCompareExchange(&g_requestQueueStopping, 0, 0)) {
            LeaveCriticalSection(&g_requestQueueLock);
            break;
        }
        LeaveCriticalSection(&g_requestQueueLock);
        if (client != INVALID_SOCKET) worker((void*)(ULONG_PTR)client);
    }
    return 0;
}

static void queueRequest(SOCKET client) {
    EnterCriticalSection(&g_requestQueueLock);
    g_requestQueue.push_back(client);
    WakeConditionVariable(&g_requestAvailable);
    LeaveCriticalSection(&g_requestQueueLock);
}

static void stopRequestPool() {
    EnterCriticalSection(&g_requestQueueLock);
    InterlockedExchange(&g_requestQueueStopping, 1);
    while (!g_requestQueue.empty()) {
        closesocket(g_requestQueue.front());
        g_requestQueue.pop_front();
    }
    WakeAllConditionVariable(&g_requestAvailable);
    LeaveCriticalSection(&g_requestQueueLock);
    for (HANDLE thread : g_requestWorkers) {
        WaitForSingleObject(thread, 2000);
        CloseHandle(thread);
    }
    g_requestWorkers.clear();
    g_transferWorkerCount = 0;
}

static std::string printLanIp(int port) { // Select an active non-loopback IPv4 address.
    ULONG bytes = 0;
    if (GetAdaptersAddresses(AF_INET, GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST,
                             NULL, NULL, &bytes) != ERROR_BUFFER_OVERFLOW) return "";
    std::vector<unsigned char> storage(bytes);
    IP_ADAPTER_ADDRESSES* adapters = (IP_ADAPTER_ADDRESSES*)storage.data();
    if (GetAdaptersAddresses(AF_INET, GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST,
                             NULL, adapters, &bytes) != NO_ERROR) return "";

    std::string fallback;
    for (IP_ADAPTER_ADDRESSES* a = adapters; a; a = a->Next) {
        if (a->OperStatus != IfOperStatusUp || a->IfType == IF_TYPE_SOFTWARE_LOOPBACK) continue;
        for (IP_ADAPTER_UNICAST_ADDRESS* u = a->FirstUnicastAddress; u; u = u->Next) {
            if (!u->Address.lpSockaddr || u->Address.lpSockaddr->sa_family != AF_INET) continue;
            char ip[INET_ADDRSTRLEN] = {};
            inet_ntop(AF_INET, &((sockaddr_in*)u->Address.lpSockaddr)->sin_addr, ip, sizeof(ip));
            if (ip[0] == 0 || !strcmp(ip, "127.0.0.1")) continue;
            if (fallback.empty()) fallback = ip;
            // Prefer a routable private LAN address over virtual adapter addresses.
            if (!strncmp(ip, "192.168.", 8) || !strncmp(ip, "10.", 3) ||
                (!strncmp(ip, "172.", 4) && atoi(ip + 4) >= 16 && atoi(ip + 4) <= 31)) {
                printf("  局域网访问: http://%s:%d/\n", ip, port);
                return ip;
            }
        }
    }
    if (!fallback.empty()) printf("  局域网访问: http://%s:%d/\n", fallback.c_str(), port);
    return fallback;
}

static HANDLE g_serverThread = NULL;

static unsigned __stdcall serverWorker(void*) {
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        InterlockedExchange(&g_serverRunning, 0);
        return 0;
    }
    SOCKET listenSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listenSocket == INVALID_SOCKET) {
        WSACleanup();
        InterlockedExchange(&g_serverRunning, 0);
        return 0;
    }
    BOOL reuse = TRUE;
    setsockopt(listenSocket, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse, sizeof(reuse));
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons((u_short)g_httpPort);
    if (bind(listenSocket, (sockaddr*)&address, sizeof(address)) != 0 || listen(listenSocket, SOMAXCONN) != 0) {
        closesocket(listenSocket);
        WSACleanup();
        InterlockedExchange(&g_serverRunning, 0);
        return 0;
    }

    g_listenSocket = listenSocket;
    InterlockedExchange(&g_requestQueueStopping, 0);
    SYSTEM_INFO systemInfo = {};
    GetSystemInfo(&systemInfo);
    int desiredWorkers = (int)systemInfo.dwNumberOfProcessors * 2;
    if (desiredWorkers < 8) desiredWorkers = 8;
    if (desiredWorkers > 16) desiredWorkers = 16;
    for (int i = 0; i < desiredWorkers; ++i) {
        HANDLE thread = (HANDLE)_beginthreadex(NULL, 0, requestPoolWorker, NULL, 0, NULL);
        if (!thread) break;
        g_requestWorkers.push_back(thread);
    }
    g_transferWorkerCount = (int)g_requestWorkers.size();
    if (!g_transferWorkerCount) {
        closesocket(listenSocket);
        g_listenSocket = INVALID_SOCKET;
        WSACleanup();
        InterlockedExchange(&g_serverRunning, 0);
        return 0;
    }
    g_localIp = printLanIp(g_httpPort);
    HANDLE discovery = NULL;
    if (!g_localIp.empty()) discovery = (HANDLE)_beginthreadex(NULL, 0, discoveryWorker, NULL, 0, NULL);

    while (InterlockedCompareExchange(&g_serverRunning, 0, 0)) {
        SOCKET client = accept(listenSocket, NULL, NULL);
        if (client == INVALID_SOCKET) {
            if (!InterlockedCompareExchange(&g_serverRunning, 0, 0)) break;
            continue;
        }
        queueRequest(client);
    }

    closesocket(listenSocket);
    g_listenSocket = INVALID_SOCKET;
    InterlockedExchange(&g_serverRunning, 0);
    stopRequestPool();
    if (discovery) { WaitForSingleObject(discovery, 1000); CloseHandle(discovery); }
    WSACleanup();
    return 0;
}

static bool startServer() {
    if (InterlockedCompareExchange(&g_serverRunning, 0, 0)) return true;
    wchar_t root[MAX_PATH * 4];
    if (!GetFullPathNameW(exeDirectory().c_str(), MAX_PATH * 4, root, NULL)) return false;
    g_root = root;
    while (g_root.size() > 3 && g_root.back() == L'\\') g_root.pop_back();
    if (GetFileAttributesW(g_root.c_str()) == INVALID_FILE_ATTRIBUTES) return false;
    g_httpPort = 8000;
    InterlockedExchange(&g_serverRunning, 1);
    g_serverThread = (HANDLE)_beginthreadex(NULL, 0, serverWorker, NULL, 0, NULL);
    if (!g_serverThread) { InterlockedExchange(&g_serverRunning, 0); return false; }
    return true;
}

static void stopServer() {
    InterlockedExchange(&g_serverRunning, 0);
    SOCKET socket = g_listenSocket;
    if (socket != INVALID_SOCKET) closesocket(socket);
    if (g_serverThread) {
        WaitForSingleObject(g_serverThread, 3000);
        CloseHandle(g_serverThread);
        g_serverThread = NULL;
    }
}

static CString displaySize(ULONGLONG value) {
    CString text;
    if (value < 1024) text.Format(L"%llu B", value);
    else if (value < 1024ull * 1024) text.Format(L"%.1f KB", value / 1024.0);
    else if (value < 1024ull * 1024 * 1024) text.Format(L"%.1f MB", value / 1048576.0);
    else text.Format(L"%.2f GB", value / 1073741824.0);
    return text;
}

class CLanShareFrame : public CFrameWnd {
public:
    CStatic m_title;
    CStatic m_status;
    CStatic m_summary;
    CStatic m_taskLabel, m_taskValue;
    CStatic m_speedLabel, m_speedValue;
    CStatic m_totalLabel, m_totalValue;
    CButton m_openButton;
    CButton m_backButton;
    CButton m_selectAllButton;
    CButton m_downloadButton;
    CListCtrl m_peers;
    CListCtrl m_remoteFiles;
    CListCtrl m_transfers;
    CStatic m_footer;
    CStatic m_emptyState;
    CStatic m_peerCaption;
    CStatic m_remoteCaption;
    CStatic m_transferCaption;
    CStatic m_remoteHint;
    CBrush m_backgroundBrush;
    CBrush m_panelBrush;
    CFont m_titleFont, m_bodyFont, m_labelFont, m_metricFont, m_listFont;
    std::map<LONG, ULONGLONG> m_lastBytes;
    ULONGLONG m_lastSample = 0;
    ULONGLONG m_lastPeerRefresh = 0;
    int m_progressValue = 0;
    std::vector<Peer> m_visiblePeers;
    std::vector<RemoteEntry> m_remoteEntries;
    Peer m_selectedPeer = {};
    std::string m_remotePath = "/";
    int m_rightClickedRemote = -1;
    bool m_refreshingPeers = false;

    std::wstring remoteRelativePrefix() const {
        if (m_remotePath == "/") return L"";
        std::string path = m_remotePath;
        while (!path.empty() && path[0] == '/') path.erase(0, 1);
        return U2W(path);
    }

    void RefreshPeerList() {
        std::string selectedIp = m_selectedPeer.ip;
        m_visiblePeers = currentPeers();
        m_refreshingPeers = true;
        m_peers.DeleteAllItems();
        int selectedRow = -1;
        for (size_t i = 0; i < m_visiblePeers.size(); ++i) {
            const Peer& peer = m_visiblePeers[i];
            CString name = U2W(peer.name).c_str();
            CString title;
            if (name.IsEmpty()) title = U2W(peer.ip).c_str();
            else title.Format(L"%s  |  %s", U2W(peer.ip).c_str(), name.GetString());
            int row = m_peers.InsertItem((int)i, title);
            if (peer.ip == selectedIp && peer.port == m_selectedPeer.port) selectedRow = row;
        }
        if (selectedRow >= 0) {
            m_peers.SetItemState(selectedRow, LVIS_SELECTED, LVIS_SELECTED);
        } else if (!m_visiblePeers.empty() && m_selectedPeer.ip.empty()) {
            m_selectedPeer = m_visiblePeers[0];
            m_peers.SetItemState(0, LVIS_SELECTED, LVIS_SELECTED);
            m_refreshingPeers = false;
            LoadRemoteDirectory();
            return;
        } else if (selectedRow < 0 && !m_selectedPeer.ip.empty()) {
            m_selectedPeer = {};
            m_remoteEntries.clear();
            PopulateRemoteFiles();
            m_remoteCaption.SetWindowText(L"对方文件");
            m_remoteHint.SetWindowText(L"所选设备已离线。");
        }
        m_refreshingPeers = false;
    }

    void PopulateRemoteFiles() {
        m_remoteFiles.DeleteAllItems();
        for (size_t i = 0; i < m_remoteEntries.size(); ++i) {
            const RemoteEntry& entry = m_remoteEntries[i];
            CString name = U2W(entry.name).c_str();
            if (entry.dir) name += L"  /";
            int row = m_remoteFiles.InsertItem((int)i, name);
            m_remoteFiles.SetItemText(row, 1, entry.dir ? L"文件夹" : L"文件");
            m_remoteFiles.SetItemText(row, 2, entry.dir ? L"-" : displaySize(entry.size));
        }
    }

    void LoadRemoteDirectory() {
        if (m_selectedPeer.ip.empty()) return;
        m_remoteHint.SetWindowText(L"正在读取对方目录...");
        std::string json;
        if (!fetchPeerList(m_selectedPeer, m_remotePath, json)) {
            m_remoteEntries.clear();
            PopulateRemoteFiles();
            m_remoteHint.SetWindowText(L"无法读取该设备的共享目录，请确认对方软件仍在运行。");
            return;
        }
        m_remoteEntries = parseRemoteEntries(json);
        PopulateRemoteFiles();
        CString caption;
        caption.Format(L"对方文件  %s", U2W(m_remotePath).c_str());
        m_remoteCaption.SetWindowText(caption);
        if (m_remoteEntries.empty()) m_remoteHint.SetWindowText(L"此文件夹为空");
        else m_remoteHint.SetWindowText(L"勾选文件或文件夹后下载；双击文件夹进入；右键可直接下载。");
    }

    void QueueSelectedDownloads(bool onlyRightClicked) {
        if (m_selectedPeer.ip.empty()) return;
        std::vector<DownloadTask> tasks;
        std::wstring prefix = remoteRelativePrefix();
        for (int row = 0; row < m_remoteFiles.GetItemCount(); ++row) {
            if (onlyRightClicked ? row != m_rightClickedRemote : !m_remoteFiles.GetCheck(row)) continue;
            if (row < 0 || (size_t)row >= m_remoteEntries.size()) continue;
            const RemoteEntry& entry = m_remoteEntries[row];
            if (entry.dir) {
                std::string manifest;
                if (fetchPeerPath(m_selectedPeer, entry.href + "?manifest=1", manifest)) {
                    std::vector<DownloadTask> folderTasks = parseManifest(m_selectedPeer, manifest, prefix + U2W(entry.name));
                    tasks.insert(tasks.end(), folderTasks.begin(), folderTasks.end());
                }
            } else {
                DownloadTask task = {};
                task.peer = m_selectedPeer;
                task.url = entry.href;
                task.relativePath = prefix + U2W(entry.name);
                tasks.push_back(task);
            }
        }
        if (tasks.empty()) {
            m_remoteHint.SetWindowText(L"没有可下载的项目，或文件夹为空。");
            return;
        }
        queueDownloads(tasks);
        CString hint;
        hint.Format(L"已加入 %d 个原始文件传输任务，保存到 downloads\\设备IP\\ 目录。", (int)tasks.size());
        m_remoteHint.SetWindowText(hint);
    }

    afx_msg int OnCreate(LPCREATESTRUCT create) {
        if (CFrameWnd::OnCreate(create) == -1) return -1;
        m_backgroundBrush.CreateSolidBrush(RGB(15, 23, 42));
        m_panelBrush.CreateSolidBrush(RGB(30, 41, 59));
        m_titleFont.CreateFontW(-24, 0, 0, 0, FW_BOLD, FALSE, FALSE, FALSE, DEFAULT_CHARSET,
                                OUT_DEFAULT_PRECIS, CLIP_DEFAULT_PRECIS, CLEARTYPE_QUALITY,
                                DEFAULT_PITCH | FF_DONTCARE, L"Microsoft YaHei");
        m_bodyFont.CreateFontW(-15, 0, 0, 0, FW_NORMAL, FALSE, FALSE, FALSE, DEFAULT_CHARSET,
                               OUT_DEFAULT_PRECIS, CLIP_DEFAULT_PRECIS, CLEARTYPE_QUALITY,
                               DEFAULT_PITCH | FF_DONTCARE, L"Microsoft YaHei");
        m_labelFont.CreateFontW(-14, 0, 0, 0, FW_SEMIBOLD, FALSE, FALSE, FALSE, DEFAULT_CHARSET,
                                OUT_DEFAULT_PRECIS, CLIP_DEFAULT_PRECIS, CLEARTYPE_QUALITY,
                                DEFAULT_PITCH | FF_DONTCARE, L"Microsoft YaHei");
        m_metricFont.CreateFontW(-18, 0, 0, 0, FW_BOLD, FALSE, FALSE, FALSE, DEFAULT_CHARSET,
                                 OUT_DEFAULT_PRECIS, CLIP_DEFAULT_PRECIS, CLEARTYPE_QUALITY,
                                 DEFAULT_PITCH | FF_DONTCARE, L"Microsoft YaHei");
        m_listFont.CreateFontW(-16, 0, 0, 0, FW_NORMAL, FALSE, FALSE, FALSE, DEFAULT_CHARSET,
                               OUT_DEFAULT_PRECIS, CLIP_DEFAULT_PRECIS, CLEARTYPE_QUALITY,
                               DEFAULT_PITCH | FF_DONTCARE, L"Microsoft YaHei");

        m_title.Create(L"局域网文件传输", WS_CHILD | WS_VISIBLE | SS_LEFT, CRect(32, 10, 300, 42), this, 1101);
        m_title.SetFont(&m_titleFont);
        m_status.Create(L"正在启动共享服务...", WS_CHILD | WS_VISIBLE | SS_LEFT, CRect(310, 15, 800, 40), this, 1102);
        m_status.SetFont(&m_bodyFont);
        m_openButton.Create(L"打开下载目录", WS_CHILD | WS_VISIBLE | BS_OWNERDRAW, CRect(866, 10, 1048, 44), this, 1001);

        m_taskLabel.Create(L"并发", WS_CHILD | WS_VISIBLE | SS_LEFT, CRect(36, 54, 84, 78), this, 1110);
        m_taskValue.Create(L"0", WS_CHILD | WS_VISIBLE | SS_LEFT, CRect(84, 52, 170, 78), this, 1120);
        m_speedLabel.Create(L"速率", WS_CHILD | WS_VISIBLE | SS_LEFT, CRect(240, 54, 288, 78), this, 1111);
        m_speedValue.Create(L"0.00 MB/s", WS_CHILD | WS_VISIBLE | SS_LEFT, CRect(288, 52, 430, 78), this, 1121);
        m_totalLabel.Create(L"已传输", WS_CHILD | WS_VISIBLE | SS_LEFT, CRect(510, 54, 575, 78), this, 1112);
        m_totalValue.Create(L"0 B", WS_CHILD | WS_VISIBLE | SS_LEFT, CRect(575, 52, 700, 78), this, 1122);
        m_taskLabel.SetFont(&m_labelFont); m_speedLabel.SetFont(&m_labelFont); m_totalLabel.SetFont(&m_labelFont);
        m_taskValue.SetFont(&m_metricFont); m_speedValue.SetFont(&m_metricFont); m_totalValue.SetFont(&m_metricFont);

        m_summary.Create(L"", WS_CHILD | SS_LEFT, CRect(720, 54, 1048, 78), this, 1103);
        m_summary.SetFont(&m_bodyFont);
        m_backButton.Create(L"上级", WS_CHILD | WS_VISIBLE | BS_OWNERDRAW, CRect(600, 104, 690, 136), this, 1005);
        m_selectAllButton.Create(L"全选", WS_CHILD | WS_VISIBLE | BS_OWNERDRAW, CRect(700, 104, 790, 136), this, 1006);
        m_downloadButton.Create(L"下载选中", WS_CHILD | WS_VISIBLE | BS_OWNERDRAW, CRect(800, 104, 930, 136), this, 1007);
        m_peerCaption.Create(L"在线设备", WS_CHILD | WS_VISIBLE | SS_LEFT, CRect(36, 104, 300, 134), this, 1130);
        m_remoteCaption.Create(L"对方文件", WS_CHILD | WS_VISIBLE | SS_LEFT, CRect(350, 104, 590, 134), this, 1131);
        m_peerCaption.SetFont(&m_labelFont); m_remoteCaption.SetFont(&m_labelFont);
        m_peers.Create(WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_NOCOLUMNHEADER | LVS_SINGLESEL | LVS_SHOWSELALWAYS,
                       CRect(36, 140, 322, 422), this, 1004);
        m_peers.SetExtendedStyle(LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER | LVS_EX_FLATSB);
        m_peers.SetFont(&m_listFont);
        m_peers.SetBkColor(RGB(30, 41, 59)); m_peers.SetTextBkColor(RGB(30, 41, 59)); m_peers.SetTextColor(RGB(226, 232, 240));
        SetWindowTheme(m_peers.GetSafeHwnd(), L"DarkMode_Explorer", NULL);
        m_peers.InsertColumn(0, L"设备", LVCFMT_LEFT, 270);
        m_remoteFiles.Create(WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_NOCOLUMNHEADER | LVS_SINGLESEL | LVS_SHOWSELALWAYS,
                             CRect(350, 140, 1048, 422), this, 1008);
        m_remoteFiles.SetExtendedStyle(LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER | LVS_EX_FLATSB | LVS_EX_CHECKBOXES);
        m_remoteFiles.SetFont(&m_listFont);
        m_remoteFiles.SetBkColor(RGB(30, 41, 59)); m_remoteFiles.SetTextBkColor(RGB(30, 41, 59)); m_remoteFiles.SetTextColor(RGB(226, 232, 240));
        SetWindowTheme(m_remoteFiles.GetSafeHwnd(), L"DarkMode_Explorer", NULL);
        m_remoteFiles.InsertColumn(0, L"名称", LVCFMT_LEFT, 410);
        m_remoteFiles.InsertColumn(1, L"类型", LVCFMT_LEFT, 100);
        m_remoteFiles.InsertColumn(2, L"大小", LVCFMT_RIGHT, 150);
        m_remoteHint.Create(L"正在发现局域网内的共享设备...", WS_CHILD | WS_VISIBLE | SS_LEFT, CRect(350, 428, 1048, 452), this, 1132);
        m_remoteHint.SetFont(&m_labelFont);
        m_transferCaption.Create(L"传输队列", WS_CHILD | WS_VISIBLE | SS_LEFT, CRect(36, 464, 300, 490), this, 1133);
        m_transferCaption.SetFont(&m_labelFont);
        m_transfers.Create(WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_NOCOLUMNHEADER | LVS_SINGLESEL | LVS_SHOWSELALWAYS,
                           CRect(36, 526, 1048, 675), this, 1003);
        m_transfers.SetExtendedStyle(LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER | LVS_EX_FLATSB);
        m_transfers.SetFont(&m_listFont);
        m_transfers.SetBkColor(RGB(30, 41, 59));
        m_transfers.SetTextBkColor(RGB(30, 41, 59));
        m_transfers.SetTextColor(RGB(226, 232, 240));
        SetWindowTheme(m_transfers.GetSafeHwnd(), L"DarkMode_Explorer", NULL);
        m_transfers.InsertColumn(0, L"文件", LVCFMT_LEFT, 260);
        m_transfers.InsertColumn(1, L"客户端", LVCFMT_LEFT, 125);
        m_transfers.InsertColumn(2, L"总大小", LVCFMT_RIGHT, 110);
        m_transfers.InsertColumn(3, L"已传输", LVCFMT_RIGHT, 110);
        m_transfers.InsertColumn(4, L"进度", LVCFMT_RIGHT, 80);
        m_transfers.InsertColumn(5, L"速率", LVCFMT_RIGHT, 100);
        m_transfers.InsertColumn(6, L"状态", LVCFMT_LEFT, 90);
        m_emptyState.Create(L"暂无传输任务", WS_CHILD | WS_VISIBLE | SS_CENTER, CRect(36, 570, 1048, 606), this, 1105);
        m_emptyState.SetFont(&m_bodyFont);
        m_footer.Create(L"共享根目录为当前 EXE 所在文件夹  ·  下载文件保存在 downloads\\设备IP\\ 目录", WS_CHILD | WS_VISIBLE | SS_LEFT,
                        CRect(36, 688, 1048, 716), this, 1104);
        m_footer.SetFont(&m_labelFont);
        SetTimer(1, 500, NULL);
        startServer();
        return 0;
    }

    afx_msg void OnTimer(UINT_PTR id) {
        if (id != 1) { CFrameWnd::OnTimer(id); return; }
        if (InterlockedCompareExchange(&g_serverRunning, 0, 0) && !g_localIp.empty()) {
            std::wstring localIp = U2W(g_localIp);
            CString status;
            status.Format(L"局域网访问地址：http://%s:%d/", localIp.c_str(), g_httpPort);
            m_status.SetWindowText(status);
        } else m_status.SetWindowText(L"共享服务未启动或正在等待局域网地址");

        ULONGLONG now = GetTickCount64();
        if (!m_lastPeerRefresh || now - m_lastPeerRefresh >= 2000) {
            RefreshPeerList();
            m_lastPeerRefresh = now;
        }

        std::vector<Transfer> transfers = currentTransfers();
        m_transfers.ShowWindow(transfers.empty() ? SW_HIDE : SW_SHOW);
        m_emptyState.ShowWindow(transfers.empty() ? SW_SHOW : SW_HIDE);
        m_transfers.DeleteAllItems();
        double sampleSeconds = m_lastSample ? (now - m_lastSample) / 1000.0 : 0.0;
        Transfer* current = NULL;
        ULONGLONG activeTotal = 0, activeSent = 0;
        double activeRate = 0.0;
        int activeCount = 0;
        for (size_t i = 0; i < transfers.size(); ++i) {
            const Transfer& transfer = transfers[i];
            CString file = U2W(transfer.file).c_str();
            int row = m_transfers.InsertItem((int)i, file);
            m_transfers.SetItemText(row, 1, U2W(transfer.client).c_str());
            m_transfers.SetItemText(row, 2, displaySize(transfer.total));
            m_transfers.SetItemText(row, 3, displaySize(transfer.sent));
            double percent = transfer.total ? (100.0 * transfer.sent / transfer.total) : 0.0;
            CString progress; progress.Format(L"%.1f%%", percent);
            m_transfers.SetItemText(row, 4, progress);
            ULONGLONG previous = m_lastBytes.count(transfer.id) ? m_lastBytes[transfer.id] : transfer.sent;
            double rateValue = sampleSeconds > 0 && transfer.sent >= previous ?
                (transfer.sent - previous) / 1048576.0 / sampleSeconds : 0.0;
            m_lastBytes[transfer.id] = transfer.sent;
            CString rate; rate.Format(L"%.2f MB/s", rateValue);
            m_transfers.SetItemText(row, 5, rate);
            m_transfers.SetItemText(row, 6, transfer.complete ? L"已完成" : L"传输中");
            if (!transfer.complete) {
                current = &transfers[i];
                activeTotal += transfer.total;
                activeSent += transfer.sent;
                activeRate += rateValue;
                ++activeCount;
            }
        }
        if (!current && !transfers.empty()) current = &transfers.back();
        if (current) {
            int value = current->total ? (int)(1000 * current->sent / current->total) : 0;
            m_progressValue = value;
            CString summary;
            CString sentText = displaySize(activeCount ? activeSent : current->sent);
            CString totalText = displaySize(activeCount ? activeTotal : current->total);
            if (activeCount) summary.Format(L"活动传输 %d 项：%s / %s，%.2f MB/s", activeCount, sentText.GetString(), totalText.GetString(), activeRate);
            else summary.Format(L"最近完成：%s / %s", sentText.GetString(), totalText.GetString());
            m_summary.SetWindowText(summary);
            m_summary.ShowWindow(activeCount ? SW_SHOW : SW_HIDE);
        } else {
            m_progressValue = 0;
            m_summary.ShowWindow(SW_HIDE);
        }
        CString taskText; taskText.Format(L"%d / %d", activeCount, g_transferWorkerCount);
        m_taskValue.SetWindowText(taskText);
        CString speedText; speedText.Format(L"%.2f MB/s", activeRate);
        m_speedValue.SetWindowText(speedText);
        m_totalValue.SetWindowText(displaySize(activeSent));
        m_lastSample = now;
        Invalidate(FALSE);
    }

    afx_msg void OnPeerChanged(NMHDR* hdr, LRESULT* result) {
        NMLISTVIEW* event = reinterpret_cast<NMLISTVIEW*>(hdr);
        if (!m_refreshingPeers && (event->uChanged & LVIF_STATE) && (event->uNewState & LVIS_SELECTED) &&
            event->iItem >= 0 && (size_t)event->iItem < m_visiblePeers.size()) {
            m_selectedPeer = m_visiblePeers[event->iItem];
            m_remotePath = "/";
            LoadRemoteDirectory();
        }
        *result = 0;
    }

    afx_msg void OnListCustomDraw(NMHDR* hdr, LRESULT* result) {
        NMLVCUSTOMDRAW* draw = reinterpret_cast<NMLVCUSTOMDRAW*>(hdr);
        if (draw->nmcd.dwDrawStage == CDDS_PREPAINT) {
            *result = CDRF_NOTIFYITEMDRAW;
            return;
        }
        if (draw->nmcd.dwDrawStage == CDDS_ITEMPREPAINT) {
            CListCtrl* list = NULL;
            if (hdr->idFrom == 1004) list = &m_peers;
            else if (hdr->idFrom == 1008) list = &m_remoteFiles;
            else if (hdr->idFrom == 1003) list = &m_transfers;
            int row = (int)draw->nmcd.dwItemSpec;
            bool selected = list && (list->GetItemState(row, LVIS_SELECTED) & LVIS_SELECTED) != 0;
            draw->clrText = selected ? RGB(255, 255, 255) : RGB(226, 232, 240);
            draw->clrTextBk = selected ? RGB(12, 104, 137) : RGB(30, 41, 59);
            *result = CDRF_NEWFONT;
            return;
        }
        *result = CDRF_DODEFAULT;
    }

    afx_msg void OnRemoteActivate(NMHDR* hdr, LRESULT* result) {
        NMITEMACTIVATE* event = reinterpret_cast<NMITEMACTIVATE*>(hdr);
        if (event->iItem >= 0 && (size_t)event->iItem < m_remoteEntries.size()) {
            const RemoteEntry& entry = m_remoteEntries[event->iItem];
            if (entry.dir) {
                m_remotePath = entry.href;
                LoadRemoteDirectory();
            } else {
                m_remoteFiles.SetCheck(event->iItem, !m_remoteFiles.GetCheck(event->iItem));
            }
        }
        *result = 0;
    }

    afx_msg void OnRemoteRightClick(NMHDR* hdr, LRESULT* result) {
        (void)hdr;
        CPoint point; GetCursorPos(&point);
        CPoint local = point; m_remoteFiles.ScreenToClient(&local);
        int row = m_remoteFiles.HitTest(local);
        if (row >= 0 && (size_t)row < m_remoteEntries.size()) {
            m_rightClickedRemote = row;
            m_remoteFiles.SetItemState(row, LVIS_SELECTED, LVIS_SELECTED);
            CMenu menu;
            menu.CreatePopupMenu();
            menu.AppendMenuW(MF_STRING, 1010, L"下载此项");
            menu.TrackPopupMenu(TPM_LEFTALIGN | TPM_RIGHTBUTTON, point.x, point.y, this);
        }
        *result = 0;
    }

    afx_msg void OnBack() {
        if (m_selectedPeer.ip.empty() || m_remotePath == "/") return;
        std::string path = m_remotePath;
        while (path.size() > 1 && path.back() == '/') path.pop_back();
        size_t slash = path.find_last_of('/');
        m_remotePath = slash == std::string::npos || slash == 0 ? "/" : path.substr(0, slash + 1);
        LoadRemoteDirectory();
    }

    afx_msg void OnSelectAll() {
        for (int row = 0; row < m_remoteFiles.GetItemCount(); ++row) m_remoteFiles.SetCheck(row, TRUE);
    }

    afx_msg void OnDownloadSelected() { QueueSelectedDownloads(false); }
    afx_msg void OnDownloadRightClick() { QueueSelectedDownloads(true); }

    afx_msg void OnOpenShare() {
        std::wstring downloads = exeDirectory() + L"\\downloads";
        SHCreateDirectoryExW(NULL, downloads.c_str(), NULL);
        ShellExecuteW(NULL, L"open", downloads.c_str(), NULL, NULL, SW_SHOWNORMAL);
    }

    afx_msg void OnSize(UINT type, int width, int height) {
        CFrameWnd::OnSize(type, width, height);
        if (!m_status.GetSafeHwnd()) return;
        int right = width - 32;
        m_status.MoveWindow(310, 15, right - 510, 25);
        m_openButton.MoveWindow(right - 182, 10, 182, 34);
        m_taskLabel.MoveWindow(36, 54, 48, 24); m_taskValue.MoveWindow(84, 52, 105, 26);
        m_speedLabel.MoveWindow(220, 54, 48, 24); m_speedValue.MoveWindow(268, 52, 165, 26);
        m_totalLabel.MoveWindow(490, 54, 70, 24); m_totalValue.MoveWindow(560, 52, 125, 26);
        m_summary.MoveWindow(710, 54, right - 710, 24);
        int browserHeight = 282;
        int remoteLeft = 350;
        if (right - remoteLeft < 380) remoteLeft = 270;
        m_peerCaption.MoveWindow(36, 104, remoteLeft - 50, 30);
        m_remoteCaption.MoveWindow(remoteLeft, 104, 260, 30);
        m_backButton.MoveWindow(right - 414, 104, 90, 32);
        m_selectAllButton.MoveWindow(right - 314, 104, 90, 32);
        m_downloadButton.MoveWindow(right - 214, 104, 182, 32);
        m_peers.MoveWindow(36, 140, remoteLeft - 62, browserHeight);
        m_remoteFiles.MoveWindow(remoteLeft, 140, right - remoteLeft, browserHeight);
        m_remoteHint.MoveWindow(remoteLeft, 428, right - remoteLeft, 24);
        m_transferCaption.MoveWindow(36, 464, 300, 26);
        int tableTop = 526;
        int tableHeight = height - tableTop - 56;
        if (tableHeight < 100) tableHeight = 100;
        m_transfers.MoveWindow(36, tableTop, right - 36, tableHeight);
        m_emptyState.MoveWindow(36, tableTop + 38, right - 36, 36);
        m_footer.MoveWindow(36, height - 34, right - 36, 24);
        m_peers.SetColumnWidth(0, max(120, remoteLeft - 82));
        int remoteWidth = right - remoteLeft;
        m_remoteFiles.SetColumnWidth(0, max(200, remoteWidth - 214));
        m_remoteFiles.SetColumnWidth(1, 78);
        m_remoteFiles.SetColumnWidth(2, 118);
    }

    afx_msg BOOL OnEraseBkgnd(CDC* dc) {
        CRect rect; GetClientRect(&rect);
        dc->FillSolidRect(rect, RGB(15, 23, 42));
        return TRUE;
    }

    afx_msg void OnPaint() {
        CPaintDC dc(this);
        CRect rect; GetClientRect(&rect);
        CBrush panel(RGB(30, 41, 59));
        CPen border(PS_SOLID, 1, RGB(51, 65, 85));
        CPen* oldPen = dc.SelectObject(&border);
        CBrush* oldBrush = dc.SelectObject(&panel);
        dc.RoundRect(24, 96, rect.Width() - 24, rect.Height() - 48, 7, 7);
        dc.SelectObject(oldBrush);
        dc.SelectObject(oldPen);

        dc.FillSolidRect(24, 86, rect.Width() - 48, 1, RGB(51, 65, 85));
        if (m_progressValue > 0) {
            CRect progress(36, 84, rect.Width() - 36, 88);
            int fillWidth = progress.Width() * m_progressValue / 1000;
            dc.FillSolidRect(progress.left, progress.top, fillWidth, progress.Height(), RGB(14, 165, 233));
        }

        CRect header(36, 490, rect.Width() - 36, 526);
        dc.FillSolidRect(header, RGB(51, 65, 85));
        dc.SetBkMode(TRANSPARENT);
        dc.SetTextColor(RGB(203, 213, 225));
        CFont* oldFont = dc.SelectObject(&m_labelFont);
        const int widths[] = { 260, 125, 110, 110, 80, 100, 90 };
        const wchar_t* labels[] = { L"文件", L"客户端", L"总大小", L"已传输", L"进度", L"速率", L"状态" };
        int x = header.left;
        for (int i = 0; i < 7; ++i) {
            CRect cell(x + 10, header.top, x + widths[i] - 8, header.bottom);
            dc.DrawText(labels[i], cell, (i == 0 || i == 1 || i == 6 ? DT_LEFT : DT_RIGHT) | DT_VCENTER | DT_SINGLELINE);
            x += widths[i];
        }
        dc.SelectObject(oldFont);
    }

    afx_msg HBRUSH OnCtlColor(CDC* dc, CWnd* wnd, UINT type) {
        if (type == CTLCOLOR_STATIC) {
            dc->SetBkMode(TRANSPARENT);
            int id = wnd->GetDlgCtrlID();
            if (id == 1101 || id == 1120 || id == 1121 || id == 1122) dc->SetTextColor(RGB(248, 250, 252));
            else if (id == 1102 || id == 1103 || id == 1104 || id == 1105 || id == 1110 || id == 1111 || id == 1112 || id == 1132) dc->SetTextColor(RGB(148, 163, 184));
            else dc->SetTextColor(RGB(226, 232, 240));
            if (id == 1105 || (id >= 1130 && id <= 1133))
                return (HBRUSH)m_panelBrush.GetSafeHandle();
            return (HBRUSH)m_backgroundBrush.GetSafeHandle();
        }
        return CFrameWnd::OnCtlColor(dc, wnd, type);
    }

    afx_msg void OnDrawItem(int id, LPDRAWITEMSTRUCT draw) {
        if (id != 1001 && id != 1005 && id != 1006 && id != 1007) { CFrameWnd::OnDrawItem(id, draw); return; }
        CDC dc; dc.Attach(draw->hDC);
        CRect rect(draw->rcItem);
        bool pressed = (draw->itemState & ODS_SELECTED) != 0;
        COLORREF normal = id == 1007 ? RGB(8, 145, 110) : RGB(14, 116, 144);
        COLORREF down = id == 1007 ? RGB(6, 95, 70) : RGB(12, 74, 110);
        dc.FillSolidRect(rect, pressed ? down : normal);
        CString label;
        GetDlgItem(id)->GetWindowText(label);
        dc.SetBkMode(TRANSPARENT); dc.SetTextColor(RGB(255, 255, 255)); dc.SelectObject(&m_bodyFont);
        dc.DrawText(label, rect, DT_CENTER | DT_VCENTER | DT_SINGLELINE);
        dc.Detach();
    }

    afx_msg void OnClose() { stopServer(); CFrameWnd::OnClose(); }

    DECLARE_MESSAGE_MAP()
};

BEGIN_MESSAGE_MAP(CLanShareFrame, CFrameWnd)
    ON_WM_CREATE()
    ON_WM_TIMER()
    ON_COMMAND(1001, OnOpenShare)
    ON_COMMAND(1005, OnBack)
    ON_COMMAND(1006, OnSelectAll)
    ON_COMMAND(1007, OnDownloadSelected)
    ON_COMMAND(1010, OnDownloadRightClick)
    ON_NOTIFY(LVN_ITEMCHANGED, 1004, OnPeerChanged)
    ON_NOTIFY(NM_CUSTOMDRAW, 1004, OnListCustomDraw)
    ON_NOTIFY(NM_DBLCLK, 1008, OnRemoteActivate)
    ON_NOTIFY(NM_RCLICK, 1008, OnRemoteRightClick)
    ON_NOTIFY(NM_CUSTOMDRAW, 1008, OnListCustomDraw)
    ON_NOTIFY(NM_CUSTOMDRAW, 1003, OnListCustomDraw)
    ON_WM_SIZE()
    ON_WM_ERASEBKGND()
    ON_WM_PAINT()
    ON_WM_CTLCOLOR()
    ON_WM_DRAWITEM()
    ON_WM_CLOSE()
END_MESSAGE_MAP()

class CLanShareApp : public CWinApp {
public:
    virtual BOOL InitInstance() {
        CWinApp::InitInstance();
        InitializeCriticalSection(&g_peerLock);
        InitializeCriticalSection(&g_transferLock);
        InitializeCriticalSection(&g_requestQueueLock);
        InitializeConditionVariable(&g_requestAvailable);
        InitializeCriticalSection(&g_downloadQueueLock);
        InitializeConditionVariable(&g_downloadAvailable);
        startDownloadPool();
        CLanShareFrame* frame = new CLanShareFrame;
        m_pMainWnd = frame;
        frame->Create(NULL, L"局域网文件传输共享", WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN,
                      CRect(160, 120, 1260, 900));
        frame->CenterWindow();
        frame->ShowWindow(SW_SHOW);
        frame->UpdateWindow();
        return TRUE;
    }
    virtual int ExitInstance() {
        stopServer();
        stopDownloadPool();
        DeleteCriticalSection(&g_downloadQueueLock);
        DeleteCriticalSection(&g_requestQueueLock);
        DeleteCriticalSection(&g_transferLock);
        DeleteCriticalSection(&g_peerLock);
        return CWinApp::ExitInstance();
    }
};

CLanShareApp theApp;
