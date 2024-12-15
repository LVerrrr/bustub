//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub
{
    // 计算给定frame_id对应的帧的k距离，即从当前时间到第k次最近访问的时间间隔
    // 如果历史记录中的访问次数少于k，则返回最大值表示无穷大
    auto LRUKReplacer::CalcKDistance(frame_id_t frame_id, const Frame &frame) -> size_t
    {
        if (frame.history_.size() < k_)
        {
            return std::numeric_limits<size_t>::max();
        }

        auto hist_iter = frame.history_.begin();
        std::advance(hist_iter, k_ - 1);

        return current_timestamp_ - *hist_iter;
    }

    // 更新指定帧的k距离
    void LRUKReplacer::UpdateKDistance(frame_id_t frame_id, Frame &frame)
    {
        frame.k_distance_ = CalcKDistance(frame_id, frame);
    }

    // 构造函数初始化LRU-K替换器，设置总帧数和k值
    LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

    // 尝试驱逐一个页框，并将页框ID存储在frame_id指针所指向的位置
    // 如果没有可驱逐的页框，则返回false
    auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool
    {
        std::scoped_lock<std::mutex> lock(latch_);

        if (curr_size_ == 0)
        {
            return false;
        }

        frame_id_t victim_id = -1;
        size_t max_k_distance = 0;
        size_t earliest_access = current_timestamp_;

        for (const auto &[fid, frame] : frames_)
        {
            if (!frame.is_evictable_)
                continue;

            if (frame.k_distance_ > max_k_distance ||
                (frame.k_distance_ == max_k_distance &&
                 !frame.history_.empty() &&
                 frame.history_.front() < earliest_access))
            {
                victim_id = fid;
                max_k_distance = frame.k_distance_;
                if (!frame.history_.empty())
                {
                    earliest_access = frame.history_.front();
                }
            }
        }

        if (victim_id != -1)
        {
            frames_.erase(victim_id);
            curr_size_--;
            *frame_id = victim_id;
            return true;
        }

        return false;
    }

    // 记录对指定frame_id的访问，并更新其历史记录和k距离
    void LRUKReplacer::RecordAccess(frame_id_t frame_id)
    {
        std::scoped_lock<std::mutex> lock(latch_);

        if (frame_id >= static_cast<frame_id_t>(replacer_size_))
        {
            throw std::runtime_error("Invalid frame_id");
        }

        auto &frame = frames_[frame_id];

        frame.history_.push_back(current_timestamp_);

        if (frame.history_.size() > k_)
        {
            frame.history_.pop_front();
        }

        UpdateKDistance(frame_id, frame);
        current_timestamp_++;
    }

    // 设置指定frame_id的帧是否可以被驱逐
    void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable)
    {
        std::scoped_lock<std::mutex> lock(latch_);

        if (frame_id >= static_cast<frame_id_t>(replacer_size_))
        {
            throw std::runtime_error("Invalid frame_id");
        }

        auto iter = frames_.find(frame_id);
        if (iter == frames_.end())
        {
            return;
        }

        if (iter->second.is_evictable_ != set_evictable)
        {
            iter->second.is_evictable_ = set_evictable;
            curr_size_ += set_evictable ? 1 : -1;
        }
    }

    // 移除指定frame_id的帧
    void LRUKReplacer::Remove(frame_id_t frame_id)
    {
        std::scoped_lock<std::mutex> lock(latch_);

        auto iter = frames_.find(frame_id);
        if (iter == frames_.end())
        {
            return;
        }

        if (!iter->second.is_evictable_)
        {
            throw std::runtime_error("Cannot remove non-evictable frame");
        }

        frames_.erase(iter);
        curr_size_--;
    }

    // 获取当前可驱逐帧的数量
    auto LRUKReplacer::Size() -> size_t
    {
        std::scoped_lock<std::mutex> lock(latch_);
        return curr_size_;
    }

} // namespace bustub