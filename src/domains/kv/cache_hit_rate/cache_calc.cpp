#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <atomic>
#include <thread>
#include <string>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

#include "lru.h"
#include "hash_util.h"

// ============================================================
// 配置
// ============================================================
struct Config {
    std::vector<std::string> file_paths;
    std::string              file_list_path;  // -L: 文件列表，每行一个路径
    std::vector<size_t>      cache_sizes;     // 0 表示无限容量
    size_t                   block_size     = 64;
    bool                     proc_timestamp = false;
    bool                     prefix_hash    = true;
    std::string              checkpoint_path;  // 空 = 不使用 checkpoint
};

// ============================================================
// 日志条目
// ============================================================
struct LogEntry {
    std::string          time;
    std::vector<int64_t> input_ids;

    bool operator<(const LogEntry& o) const { return time < o.time; }
};

// ============================================================
// 快速解析 input_ids（指针扫描 + strtoll，无 substr / istringstream）
// ============================================================
static std::vector<int64_t> parse_input_ids(const std::string& line) {
    std::vector<int64_t> ids;
    const char* p = line.c_str();

    // 定位 'input_ids': [
    const char* tag = strstr(p, "'input_ids': [");
    if (!tag) return ids;
    p = tag + 14;  // strlen("'input_ids': [")

    // 预分配，减少扩容
    ids.reserve(256);

    while (*p && *p != ']') {
        // 跳过空白和逗号
        while (*p == ' ' || *p == ',' || *p == '\t') ++p;
        if (*p == ']' || *p == '\0') break;

        char* end = nullptr;
        int64_t val = strtoll(p, &end, 10);
        if (end == p) break;  // 无法解析
        ids.push_back(val);
        p = end;
    }
    return ids;
}

// ============================================================
// 提取时间戳 (YYYY-MM-DD HH:MM:SS)
// ============================================================
static std::string parse_timestamp(const std::string& line) {
    const char* p = line.c_str();
    size_t len = line.size();
    for (size_t i = 0; i + 19 <= len; ++i) {
        if (p[i] >= '0' && p[i] <= '9' &&
            p[i+4] == '-' && p[i+7] == '-' &&
            p[i+10] == ' ' && p[i+13] == ':' && p[i+16] == ':') {
            return line.substr(i, 19);
        }
    }
    return "";
}

// ============================================================
// 对单个 entry 执行 block 哈希 + LRU 缓存模拟
// ============================================================
static void process_entry(const std::vector<int64_t>& token_list,
                           size_t block_size, bool prefix_hash,
                           std::vector<lru::LRUCache<uint64_t>>& caches) {
    size_t token_count = token_list.size();
    if (token_count == 0) return;

    size_t num_blocks = token_count / block_size + (token_count % block_size ? 1 : 0);
    std::vector<uint64_t> block_hashes(num_blocks);

    hash_utils::get_block_hash_ids(token_list.data(), token_count,
                                    block_size, block_hashes.data(), prefix_hash);

    for (uint64_t bh : block_hashes) {
        for (auto& cache : caches) {
            cache.Add(bh);
        }
    }
}

// ============================================================
// 读取单个文件的所有 entry（用于需要排序的场景）
// ============================================================
static std::vector<LogEntry> read_all_entries(const std::string& filename,
                                               bool proc_timestamp,
                                               size_t& token_count_out) {
    std::vector<LogEntry> entries;
    token_count_out = 0;
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Error: cannot open file " << filename << std::endl;
        return entries;
    }
    std::string line;
    while (std::getline(file, line)) {
        std::string ts;
        if (proc_timestamp) {
            ts = parse_timestamp(line);
            if (ts.empty()) continue;
        }
        auto ids = parse_input_ids(line);
        if (ids.empty()) continue;
        token_count_out += ids.size();
        entries.push_back({std::move(ts), std::move(ids)});
    }
    return entries;
}

// ============================================================
// 段标记常量
// ============================================================
static const char* SECTION_PREFIX = "__SECTION__:";

// ============================================================
// 输出所有 cache 的段统计（遇到段边界时调用）
// ============================================================
static void emit_section_stats(const std::string& section_name,
                                std::vector<lru::LRUCache<uint64_t>>& caches) {
    for (const auto& cache : caches) {
        if (cache.getSectionAdds() == 0) continue;
        std::cout << "section: " << section_name
                  << "\tcache_size: " << cache.getCapacity()
                  << "\tsection_adds: " << cache.getSectionAdds()
                  << "\tsection_hits: " << cache.getSectionHits()
                  << "\tsection_hit_rate: " << cache.getSectionHitRate()
                  << std::endl;
    }
}

// ============================================================
// 流式处理输入流（通用版本，支持文件和 stdin）
// 支持 __SECTION__:<name> 分段标记：
//   标记表示新段的开始，此前累积的数据属于上一段。
//   输出上一段统计，重置段计数器，保留 LRU 状态。
// ============================================================
static void stream_process_istream(std::istream& input,
                                     const Config& cfg,
                                     std::vector<lru::LRUCache<uint64_t>>& caches,
                                     std::atomic<size_t>& total_tokens,
                                     std::atomic<size_t>& current_tokens,
                                     std::atomic<size_t>& entry_count,
                                     std::string& current_section,
                                     bool& has_sections) {
    std::string line;
    while (std::getline(input, line)) {
        // 检查段标记
        if (line.find(SECTION_PREFIX) == 0) {
            std::string new_section = line.substr(strlen(SECTION_PREFIX));
            has_sections = true;
            // 输出上一段的统计（如果有的话）
            if (!current_section.empty()) {
                emit_section_stats(current_section, caches);
            }
            // 重置段计数器（保留 LRU cache 状态）
            for (auto& cache : caches) {
                cache.resetSection();
            }
            current_section = std::move(new_section);
            continue;
        }
        auto ids = parse_input_ids(line);
        if (ids.empty()) continue;
        total_tokens += ids.size();
        ++entry_count;
        process_entry(ids, cfg.block_size, cfg.prefix_hash, caches);
        current_tokens += ids.size();
    }
}

// ============================================================
// 流式处理单个文件（不需要排序时，逐行读取 + 处理，不占额外内存）
// ============================================================
static void stream_process_file(const std::string& filename,
                                 const Config& cfg,
                                 std::vector<lru::LRUCache<uint64_t>>& caches,
                                 std::atomic<size_t>& total_tokens,
                                 std::atomic<size_t>& current_tokens,
                                 std::atomic<size_t>& entry_count,
                                 std::string& current_section,
                                 bool& has_sections) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Error: cannot open file " << filename << std::endl;
        return;
    }
    stream_process_istream(file, cfg, caches,
                            total_tokens, current_tokens, entry_count,
                            current_section, has_sections);
}

// ============================================================
// 进度打印线程
// ============================================================
static void progress_thread_func(const std::atomic<size_t>& current,
                                  const std::atomic<size_t>& total,
                                  const std::atomic<bool>& done) {
    while (!done.load(std::memory_order_relaxed)) {
        size_t t = total.load(std::memory_order_relaxed);
        size_t c = current.load(std::memory_order_relaxed);
        if (t > 0) {
            std::cout << "progress: " << c << " / " << t
                      << " (" << (c * 100.0 / t) << "%)" << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

// ============================================================
// 用法说明
// ============================================================
static void print_usage(const char* prog) {
    std::cout << "Usage: " << prog
              << " -f <file> [-f <file2> ...] -s <cache_size> [-s <size2> ...] "
                 "[-b <block_size>] [-t true] [-p false] [-c <checkpoint>] [-L <file_list>]\n"
              << "\n"
              << "  -f  input file path (repeatable for multi-file merge; '-' for stdin)\n"
              << "  -L  file list path: each line is a file path, auto-insert __SECTION__ per file\n"
              << "  -s  max block count for LRU cache; 0 = unlimited (repeatable)\n"
              << "  -b  block size in tokens (default: 64)\n"
              << "  -t  process timestamps for sorting (default: false)\n"
              << "  -p  use prefix hash (default: true; -p 0 or -p false to disable)\n"
              << "  -c  checkpoint file path for saving/loading LRU cache state\n";
}

// ============================================================
// main
// ============================================================
int main(int argc, char* argv[]) {
    Config cfg;

    int o;
    const char* optstring = "f:s:t:p:b:c:L:h";
    while ((o = getopt(argc, argv, optstring)) != -1) {
        switch (o) {
        case 'f': cfg.file_paths.push_back(optarg); break;
        case 's': cfg.cache_sizes.push_back(static_cast<size_t>(std::stoi(optarg))); break;
        case 'b': cfg.block_size = static_cast<size_t>(std::stoi(optarg)); break;
        case 't':
            if (strcmp(optarg, "1") == 0 || strcmp(optarg, "true") == 0)
                cfg.proc_timestamp = true;
            break;
        case 'p':
            if (strcmp(optarg, "0") == 0 || strcmp(optarg, "false") == 0)
                cfg.prefix_hash = false;
            break;
        case 'c': cfg.checkpoint_path = optarg; break;
        case 'L': cfg.file_list_path = optarg; break;
        case 'h': print_usage(argv[0]); return 0;
        default:  print_usage(argv[0]); return 1;
        }
    }

    // 从 -L 文件列表加载路径
    if (!cfg.file_list_path.empty()) {
        std::ifstream list_file(cfg.file_list_path);
        if (!list_file.is_open()) {
            std::cerr << "Error: cannot open file list: " << cfg.file_list_path << std::endl;
            return 1;
        }
        std::string line;
        while (std::getline(list_file, line)) {
            // 去除首尾空白
            size_t start = line.find_first_not_of(" \t\r\n");
            size_t end = line.find_last_not_of(" \t\r\n");
            if (start == std::string::npos) continue;  // 空行
            cfg.file_paths.push_back(line.substr(start, end - start + 1));
        }
        std::cerr << "[filelist] loaded " << cfg.file_paths.size()
                  << " files from " << cfg.file_list_path << std::endl;
    }

    if (cfg.file_paths.empty() || cfg.cache_sizes.empty()) {
        print_usage(argv[0]);
        return 1;
    }

    // 构建 LRU 缓存实例
    std::vector<lru::LRUCache<uint64_t>> caches;
    caches.reserve(cfg.cache_sizes.size());
    for (size_t sz : cfg.cache_sizes) {
        caches.emplace_back(sz);
    }

    // ---- 加载 checkpoint（如果指定且文件存在）----
    if (!cfg.checkpoint_path.empty()) {
        for (size_t i = 0; i < caches.size(); ++i) {
            std::string cp_path = caches.size() == 1
                ? cfg.checkpoint_path
                : cfg.checkpoint_path + "." + std::to_string(i);
            if (caches[i].loadCheckpoint(cp_path)) {
                std::cerr << "[checkpoint] loaded: " << cp_path
                          << " (cache_size=" << caches[i].getCapacity()
                          << ", entries=" << caches[i].size()
                          << ", total_adds=" << caches[i].getTotalAdds()
                          << ", hit_count=" << caches[i].getHitCount() << ")" << std::endl;
            } else {
                std::cerr << "[checkpoint] no valid checkpoint at " << cp_path
                          << ", starting from empty cache" << std::endl;
            }
        }
    }

    std::atomic<size_t> total_tokens(0);
    std::atomic<size_t> current_tokens(0);
    std::atomic<size_t> entry_count(0);
    std::atomic<bool>   done(false);
    std::string         current_section;
    bool                has_sections = false;

    // 启动进度线程
    std::thread progress_thr(progress_thread_func,
                              std::cref(current_tokens),
                              std::cref(total_tokens),
                              std::cref(done));

    // 判断是否需要全量加载（多文件 + 时间排序）
    bool need_sort = cfg.proc_timestamp && cfg.file_paths.size() > 1;

    if (need_sort) {
        // 多文件按时间排序：并行读取，合并排序后处理
        std::vector<std::vector<LogEntry>> all_entries(cfg.file_paths.size());
        std::vector<size_t> file_token_counts(cfg.file_paths.size(), 0);
        std::vector<std::thread> readers;

        for (size_t i = 0; i < cfg.file_paths.size(); ++i) {
            readers.emplace_back([&, i]() {
                all_entries[i] = read_all_entries(cfg.file_paths[i],
                                                   cfg.proc_timestamp,
                                                   file_token_counts[i]);
            });
        }
        for (auto& t : readers) t.join();

        // 合并
        std::vector<LogEntry> entries;
        for (size_t i = 0; i < all_entries.size(); ++i) {
            total_tokens += file_token_counts[i];
            entries.insert(entries.end(),
                           std::make_move_iterator(all_entries[i].begin()),
                           std::make_move_iterator(all_entries[i].end()));
            all_entries[i].clear();
        }
        entry_count = entries.size();
        std::sort(entries.begin(), entries.end());

        std::cout << "entries: " << entries.size()
                  << ", tokens: " << total_tokens.load() << std::endl;

        for (const auto& e : entries) {
            process_entry(e.input_ids, cfg.block_size, cfg.prefix_hash, caches);
            current_tokens += e.input_ids.size();
        }
    } else {
        // 流式处理：逐文件逐行，内存友好
        bool has_stdin = (cfg.file_paths.size() == 1 && cfg.file_paths[0] == "-");
        if (has_stdin) {
            // stdin 模式：从标准输入读取（配合管道使用，避免 CFS 文件往返）
            stream_process_istream(std::cin, cfg, caches,
                                    total_tokens, current_tokens, entry_count,
                                    current_section, has_sections);
        } else {
            // 多文件模式：每个文件自动插入 __SECTION__ 标记
            // section 名 = 文件名（去掉目录和 _input_ids.txt 后缀）
            for (size_t fi = 0; fi < cfg.file_paths.size(); ++fi) {
                const auto& path = cfg.file_paths[fi];

                // 从文件路径提取 section 名
                std::string basename = path;
                size_t slash = basename.find_last_of('/');
                if (slash != std::string::npos) basename = basename.substr(slash + 1);
                // 去掉 _input_ids.txt 后缀
                const char* suffix = "_input_ids.txt";
                size_t slen = strlen(suffix);
                if (basename.size() > slen && basename.substr(basename.size() - slen) == suffix) {
                    basename = basename.substr(0, basename.size() - slen);
                }

                // 触发上一段的统计输出（如果有）
                if (!current_section.empty()) {
                    emit_section_stats(current_section, caches);
                }
                // 重置段计数器
                for (auto& cache : caches) {
                    cache.resetSection();
                }
                current_section = basename;
                has_sections = true;

                // 流式处理该文件
                stream_process_file(path, cfg, caches,
                                    total_tokens, current_tokens, entry_count,
                                    current_section, has_sections);

                // 进度输出（每 100 个文件）
                if ((fi + 1) % 100 == 0 || fi + 1 == cfg.file_paths.size()) {
                    std::cerr << "[progress] " << (fi + 1) << "/" << cfg.file_paths.size()
                              << " files processed" << std::endl;
                }
            }
        }
        std::cout << "entries: " << entry_count.load()
                  << ", tokens: " << total_tokens.load() << std::endl;
    }

    done.store(true, std::memory_order_relaxed);
    progress_thr.join();

    // 输出最后一段的统计
    if (has_sections && !current_section.empty()) {
        emit_section_stats(current_section, caches);
    }

    // 输出结果（全局统计，向后兼容）
    for (const auto& cache : caches) {
        std::cout << "cache_size: " << cache.getCapacity()
                  << "\ttotal_adds: " << cache.getTotalAdds()
                  << "\thit_count: " << cache.getHitCount()
                  << "\thit_rate: " << cache.getHitRate()
                  << std::endl;
    }

    // ---- 保存 checkpoint（如果指定）----
    if (!cfg.checkpoint_path.empty()) {
        for (size_t i = 0; i < caches.size(); ++i) {
            std::string cp_path = caches.size() == 1
                ? cfg.checkpoint_path
                : cfg.checkpoint_path + "." + std::to_string(i);
            if (caches[i].saveCheckpoint(cp_path)) {
                std::cerr << "[checkpoint] saved: " << cp_path
                          << " (entries=" << caches[i].size() << ")" << std::endl;
            } else {
                std::cerr << "[checkpoint] ERROR: failed to save " << cp_path << std::endl;
            }
        }
    }

    return 0;
}
