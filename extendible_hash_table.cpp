//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub
{
  // 构造函数初始化哈希表，设置桶大小并创建一个初始桶
  template <typename K, typename V>
  ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size)
  {
    auto bucket = std::make_shared<Bucket>(bucket_size, 0);
    dir_.push_back(bucket); // 将新创建的桶添加到目录中
  }

  // 根据键计算出应该放置在哪个桶的索引
  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t
  {
    int mask = (1 << global_depth_) - 1; // 计算掩码用于确定索引
    return std::hash<K>()(key) & mask;   // 返回哈希值与掩码相与后的结果作为索引

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int
    {
      std::scoped_lock<std::mutex> lock(latch_);
      return GetGlobalDepthInternal();
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int
    {
      return global_depth_;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int
    {
      std::scoped_lock<std::mutex> lock(latch_);
      return GetLocalDepthInternal(dir_index);
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int
    {
      return dir_[dir_index]->GetDepth();
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int
    {
      std::scoped_lock<std::mutex> lock(latch_);
      return GetNumBucketsInternal();
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int
    {
      return num_buckets_;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool
    {
      std::scoped_lock<std::mutex> lock(latch_);

      auto bucket = dir_[IndexOf(key)];

      return bucket->Find(key, value);
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool
    {
      std::scoped_lock<std::mutex> lock(latch_);

      auto bucket = dir_[IndexOf(key)];

      return bucket->Remove(key);
    }

    // 插入键值对
    template <typename K, typename V>
    void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value)
    {
      std::scoped_lock<std::mutex> lock(latch_); // 锁定以确保线程安全

      // 如果目标桶已满，则需要分裂该桶
      while (dir_[IndexOf(key)]->IsFull())
      {
        auto index = IndexOf(key);
        auto bucket = dir_[index];

        // 如果桶的深度等于全局深度，则需要增加全局深度，并扩展目录
        if (bucket->GetDepth() == global_depth_)
        {
          global_depth_++;
          int dir_num = dir_.size();
          dir_.resize(dir_num * 2); // 扩展目录
          for (int i = 0; i < dir_num; i++)
          {
            dir_[i + dir_num] = dir_[i]; // 复制现有目录项到新的位置
          }
        }

        int mask = 1 << bucket->GetDepth(); // 计算掩码

        bucket->IncrementDepth();                                                      // 增加桶的深度
        auto new_bucket1 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth()); // 创建第一个新桶
        auto new_bucket2 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth()); // 创建第二个新桶
        num_buckets_++;                                                                // 桶数量加一

        // 将旧桶中的元素重新分配到两个新桶中
        for (const auto &item : bucket->GetItems())
        {
          size_t hash_key = std::hash<K>()(item.first) & mask;
          if (hash_key == 0U)
          {
            new_bucket1->Insert(item.first, item.second); // 插入到第一个新桶
          }
          else
          {
            new_bucket2->Insert(item.first, item.second); // 插入到第二个新桶
          }
        }

        // 更新目录指向新桶
        for (size_t i = 0; i < dir_.size(); i++)
        {
          if (dir_[i] == bucket)
          {
            if ((i & mask) == 0U)
            {
              dir_[i] = new_bucket1; // 将目录更新为指向第一个新桶
            }
            else
            {
              dir_[i] = new_bucket2; // 将目录更新为指向第二个新桶
            }
          }
        }
      }

      auto bucket = dir_[IndexOf(key)]; // 获取最终的桶

      // 如果已经存在相同的键，则更新其值
      for (auto &item : bucket->GetItems())
      {
        if (item.first == key)
        {
          item.second = value;
          return;
        }
      }

      bucket->Insert(key, value); // 向桶中插入新的键值对
    }

    //===--------------------------------------------------------------------===//
    // Bucket
    //===--------------------------------------------------------------------===//
    template <typename K, typename V>
    ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool
    {
      for (std::pair<K, V> pair : list_)
      {
        if (pair.first == key)
        {
          value = pair.second;
          return true;
        }
      }
      return false;
    }

    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool
    {
      for (std::pair<K, V> pair : list_)
      {
        if (pair.first == key)
        {
          list_.remove(pair);
          return true;
        }
      }
      return false;
    }

    // 向桶中插入键值对
    template <typename K, typename V>
    auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool
    {
      if (IsFull())
      {
        return false; // 如果桶已满则返回false
      }
      list_.emplace_back(key, value);
      return true; // 否则插入键值对，并返回true
    }

    template class ExtendibleHashTable<page_id_t, Page *>;
    template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
    template class ExtendibleHashTable<int, int>;
    // test purpose
    template class ExtendibleHashTable<int, std::string>;
    template class ExtendibleHashTable<int, std::list<int>::iterator>;

  } // namespace bustub
