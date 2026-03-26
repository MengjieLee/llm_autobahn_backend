#ifndef LRU_H
#define LRU_H

#include <cstddef>
#include <list>
#include <unordered_map>

namespace lru {

template<typename T>
class LRUCache {
public:
    explicit LRUCache(size_t cap)
        : capacity_(cap), total_adds_(0), hit_count_(0) {}

    // 添加元素，返回是否命中缓存
    bool Add(T key) {
        ++total_adds_;
        auto it = index_.find(key);
        if (it != index_.end()) {
            // 命中：移到链表尾部（最近使用）
            ++hit_count_;
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

    double getHitRate() const {
        if (total_adds_ == 0) return 0.0;
        return static_cast<double>(hit_count_) / total_adds_;
    }

private:
    size_t capacity_;
    size_t total_adds_;
    size_t hit_count_;
    std::list<T> order_;
    std::unordered_map<T, typename std::list<T>::iterator> index_;
};

} // namespace lru

#endif
