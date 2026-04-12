#ifndef LRU_H
#define LRU_H

#include <cstddef>
#include <list>
#include <unordered_map>
#include <fstream>
#include <string>
#include <cstdint>

namespace lru {

// Checkpoint 文件魔数 + 版本号
static const uint32_t CHECKPOINT_MAGIC = 0x4C525543;  // "LRUC"
static const uint32_t CHECKPOINT_VERSION = 1;

template<typename T>
class LRUCache {
public:
    explicit LRUCache(size_t cap)
        : capacity_(cap), total_adds_(0), hit_count_(0),
          section_adds_(0), section_hits_(0) {}

    // 添加元素，返回是否命中缓存
    bool Add(T key) {
        ++total_adds_;
        ++section_adds_;
        auto it = index_.find(key);
        if (it != index_.end()) {
            // 命中：移到链表尾部（最近使用）
            ++hit_count_;
            ++section_hits_;
            order_.splice(order_.end(), order_, it->second);
            return true;
        }
        // 未命中：插入新元素
        if (capacity_ != 0 && index_.size() >= capacity_) {
            // 淘汰链表头部（最久未使用）
            index_.erase(order_.front());
            order_.pop_front();
        }
        order_.push_back(key);
        index_[key] = std::prev(order_.end());
        return false;
    }

    size_t getCapacity() const { return capacity_; }
    size_t getTotalAdds() const { return total_adds_; }
    size_t getHitCount() const { return hit_count_; }
    size_t size() const { return index_.size(); }

    double getHitRate() const {
        if (total_adds_ == 0) return 0.0;
        return static_cast<double>(hit_count_) / total_adds_;
    }

    // 段级计数器（用于 per-minute / per-section 统计）
    void resetSection() {
        section_adds_ = 0;
        section_hits_ = 0;
    }
    size_t getSectionAdds() const { return section_adds_; }
    size_t getSectionHits() const { return section_hits_; }
    double getSectionHitRate() const {
        if (section_adds_ == 0) return 0.0;
        return static_cast<double>(section_hits_) / section_adds_;
    }

    // ---- Checkpoint 持久化 ----

    /**
     * 保存 LRU cache 状态到二进制文件。
     * 格式: [magic(4B)] [version(4B)] [capacity(8B)] [total_adds(8B)] [hit_count(8B)]
     *       [num_keys(8B)] [key_0(8B)] [key_1(8B)] ...
     * 顺序与 order_ 链表一致（从最旧到最新）。
     */
    bool saveCheckpoint(const std::string& path) const {
        std::string tmp_path = path + ".tmp";
        std::ofstream out(tmp_path, std::ios::binary);
        if (!out) return false;

        uint32_t magic = CHECKPOINT_MAGIC;
        uint32_t version = CHECKPOINT_VERSION;
        uint64_t num_keys = order_.size();

        out.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
        out.write(reinterpret_cast<const char*>(&version), sizeof(version));
        out.write(reinterpret_cast<const char*>(&capacity_), sizeof(capacity_));
        out.write(reinterpret_cast<const char*>(&total_adds_), sizeof(total_adds_));
        out.write(reinterpret_cast<const char*>(&hit_count_), sizeof(hit_count_));
        out.write(reinterpret_cast<const char*>(&num_keys), sizeof(num_keys));

        for (const auto& key : order_) {
            out.write(reinterpret_cast<const char*>(&key), sizeof(key));
        }

        out.flush();
        if (!out.good()) {
            std::remove(tmp_path.c_str());
            return false;
        }
        out.close();

        // 原子替换
        if (std::rename(tmp_path.c_str(), path.c_str()) != 0) {
            std::remove(tmp_path.c_str());
            return false;
        }
        return true;
    }

    /**
     * 从 checkpoint 文件恢复 LRU cache 状态。
     * 成功返回 true，失败返回 false（cache 保持原状态）。
     */
    bool loadCheckpoint(const std::string& path) {
        std::ifstream in(path, std::ios::binary);
        if (!in) return false;

        uint32_t magic = 0, version = 0;
        in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        in.read(reinterpret_cast<char*>(&version), sizeof(version));
        if (magic != CHECKPOINT_MAGIC || version != CHECKPOINT_VERSION) {
            return false;
        }

        size_t new_capacity = 0, new_total_adds = 0, new_hit_count = 0;
        uint64_t num_keys = 0;
        in.read(reinterpret_cast<char*>(&new_capacity), sizeof(new_capacity));
        in.read(reinterpret_cast<char*>(&new_total_adds), sizeof(new_total_adds));
        in.read(reinterpret_cast<char*>(&new_hit_count), sizeof(new_hit_count));
        in.read(reinterpret_cast<char*>(&num_keys), sizeof(num_keys));

        if (!in.good()) return false;

        // 验证：capacity 必须匹配当前 cache 配置
        if (new_capacity != capacity_) {
            std::cerr << "[checkpoint] capacity mismatch: checkpoint=" << new_capacity
                      << " current=" << capacity_ << ", skipping" << std::endl;
            return false;
        }

        // 读取 keys 并重建 LRU 状态
        std::list<T> new_order;
        std::unordered_map<T, typename std::list<T>::iterator> new_index;
        new_index.reserve(num_keys);

        for (uint64_t i = 0; i < num_keys; ++i) {
            T key;
            in.read(reinterpret_cast<char*>(&key), sizeof(key));
            if (!in.good()) return false;
            new_order.push_back(key);
            new_index[key] = std::prev(new_order.end());
        }

        // 提交更新
        order_ = std::move(new_order);
        index_ = std::move(new_index);
        total_adds_ = new_total_adds;
        hit_count_ = new_hit_count;
        section_adds_ = 0;
        section_hits_ = 0;
        return true;
    }

private:
    size_t capacity_;
    size_t total_adds_;
    size_t hit_count_;
    size_t section_adds_;
    size_t section_hits_;
    std::list<T> order_;
    std::unordered_map<T, typename std::list<T>::iterator> index_;
};

} // namespace lru

#endif
