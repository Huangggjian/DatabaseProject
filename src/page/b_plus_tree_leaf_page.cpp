/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>
#include <include/page/b_plus_tree_internal_page.h>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb
{

    /*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

    /**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id)
    {
        //init过程和internal_page一样
        SetPageType(IndexPageType::LEAF_PAGE);
        SetSize(0);
        assert(sizeof(BPlusTreeLeafPage) == 28);
        SetPageId(page_id);
        SetParentPageId(parent_id);
        SetNextPageId(INVALID_PAGE_ID);                                                //要给下一个page赋初始值！！
        SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / sizeof(MappingType) - 1); //minus 1 for insert first then split （这里减1是为了当哨兵，方便插入分裂）
    }

    /**
 * Helper methods to set/get next page id
 */
    INDEX_TEMPLATE_ARGUMENTS
    page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const
    {
        return next_page_id_;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

    /**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex( //找到第一个下标i, 使得array[i].first >= key
        const KeyType &key, const KeyComparator &comparator) const
    {
        assert(GetSize() >= 0);
        int st = 0, ed = GetSize() - 1;
        while (st <= ed)
        { //find the last key in array <= input
            int mid = (ed - st) / 2 + st;
            if (comparator(array[mid].first, key) >= 0)
                ed = mid - 1;
            else
                st = mid + 1;
        }
        return ed + 1;
    }

    /*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
    INDEX_TEMPLATE_ARGUMENTS
    KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const
    {
        assert(index >= 0 && index < GetSize());
        return array[index].first;
    }

    /*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
    INDEX_TEMPLATE_ARGUMENTS
    const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index)
    {
        assert(index >= 0 && index < GetSize());
        return array[index];
    }

    /*****************************************************************************
 * INSERTION
 *****************************************************************************/
    /*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                           const ValueType &value,
                                           const KeyComparator &comparator)
    {
        int idx = KeyIndex(key, comparator); //first larger than key
        assert(idx >= 0);
        IncreaseSize(1);
        int curSize = GetSize();
        for (int i = curSize - 1; i > idx; i--)
        {
            array[i].first = array[i - 1].first;
            array[i].second = array[i - 1].second;
        }
        array[idx].first = key;
        array[idx].second = value;
        return curSize;
    }

    /*****************************************************************************
 * SPLIT
 *****************************************************************************/
    /*
 * Remove half of key & value pairs from this page to "recipient" page
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
        BPlusTreeLeafPage *recipient,
        __attribute__((unused)) BufferPoolManager *buffer_pool_manager)
    {
        assert(recipient != nullptr);
        int total = GetMaxSize() + 1;
        assert(GetSize() == total);
        //copy last half
        int copyIdx = (total) / 2; //7 is 4,5,6,7; 8 is 4,5,6,7,8
        for (int i = copyIdx; i < total; i++)
        {
            recipient->array[i - copyIdx].first = array[i].first;
            recipient->array[i - copyIdx].second = array[i].second;
        }
        //set pointer
        //新的page(split出来的page)的nextpageid就是旧的page(被split)的nextpageid
        recipient->SetNextPageId(GetNextPageId());
        //同时旧的page的nextpageid就是新的page id
        SetNextPageId(recipient->GetPageId());
        //给新旧page重新设置size
        SetSize(copyIdx);
        recipient->SetSize(total - copyIdx);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {}

    /*****************************************************************************
 * LOOKUP
 *****************************************************************************/
    /*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                            const KeyComparator &comparator) const
    {
        int idx = KeyIndex(key, comparator); //二分法
        if (idx < GetSize() && comparator(array[idx].first, key) == 0)
        {
            value = array[idx].second; //存结果
            return true;
        }
        return false;
    }

    /*****************************************************************************
 * REMOVE
 *****************************************************************************/
    /*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
        const KeyType &key, const KeyComparator &comparator)
    {
        int firIdxLargerEqualThanKey = KeyIndex(key, comparator);                                           //找到要删除的key在leaf page的下标
        if (firIdxLargerEqualThanKey >= GetSize() || comparator(key, KeyAt(firIdxLargerEqualThanKey)) != 0) //找不到要删除的key
        {
            return GetSize(); //代表没有删除，size不变的
        }
        //quick deletion
        int tarIdx = firIdxLargerEqualThanKey;
        memmove(array + tarIdx, array + tarIdx + 1, //被删除的key-val后的所有key-value向前移动一个单位
                static_cast<size_t>((GetSize() - tarIdx - 1) * sizeof(MappingType)));
        IncreaseSize(-1); //size - 1
        return GetSize(); //返回删除后的size
    }

    /*****************************************************************************
 * MERGE
 *****************************************************************************/
    /*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
    //叶子节点的比较简单粗暴，就是直接移动，然后更新NEXT PAGE ID即可。和MOVE HALF TO 差不多的套路，复制下来改改就好。
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                               int, BufferPoolManager *)
    {
        assert(recipient != nullptr);

        //copy last half
        int startIdx = recipient->GetSize(); //7 is 4,5,6,7; 8 is 4,5,6,7,8
        for (int i = 0; i < GetSize(); i++)
        {
            recipient->array[startIdx + i].first = array[i].first;
            recipient->array[startIdx + i].second = array[i].second;
        }
        //set pointer
        recipient->SetNextPageId(GetNextPageId());
        //set size, is odd, bigger is last part
        //接收页的size增加
        recipient->IncreaseSize(GetSize());
        //全部移走了，size=0
        SetSize(0);
    }
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {}

    /*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
    /*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
        BPlusTreeLeafPage *recipient,
        BufferPoolManager *buffer_pool_manager)
    {
        MappingType pair = GetItem(0);
        IncreaseSize(-1);
        memmove(array, array + 1, static_cast<size_t>(GetSize() * sizeof(MappingType)));
        recipient->CopyLastFrom(pair);
        //update relavent key & value pair in its parent page.
        Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
        B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
        parent->SetKeyAt(parent->ValueIndex(GetPageId()), array[0].first);
        buffer_pool_manager->UnpinPage(GetParentPageId(), true);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item)
    {
        assert(GetSize() + 1 <= GetMaxSize());
        array[GetSize()] = item;
        IncreaseSize(1);
    }
    /*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
        BPlusTreeLeafPage *recipient, int parentIndex,
        BufferPoolManager *buffer_pool_manager)
    {
        MappingType pair = GetItem(GetSize() - 1); //最后一个
        IncreaseSize(-1);
        recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager); //移到接收方的第一个位置上去
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
        const MappingType &item, int parentIndex,
        BufferPoolManager *buffer_pool_manager)
    {
        assert(GetSize() + 1 < GetMaxSize());
        memmove(array + 1, array, GetSize() * sizeof(MappingType));
        IncreaseSize(1);
        array[0] = item;

        Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
        B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
        parent->SetKeyAt(parentIndex, array[0].first);
        buffer_pool_manager->UnpinPage(GetParentPageId(), true);
    }

    /*****************************************************************************
 * DEBUG
 *****************************************************************************/
    INDEX_TEMPLATE_ARGUMENTS
    std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const
    {
        if (GetSize() == 0)
        {
            return "";
        }
        std::ostringstream stream;
        if (verbose)
        {
            stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
                   << "]<" << GetSize() << "> ";
        }
        int entry = 0;
        int end = GetSize();
        bool first = true;

        while (entry < end)
        {
            if (first)
            {
                first = false;
            }
            else
            {
                stream << " ";
            }
            stream << std::dec << array[entry].first;
            if (verbose)
            {
                stream << "(" << array[entry].second << ")";
            }
            ++entry;
        }
        return stream.str();
    }

    template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                     GenericComparator<4>>;
    template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                     GenericComparator<8>>;
    template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                     GenericComparator<16>>;
    template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                     GenericComparator<32>>;
    template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                     GenericComparator<64>>;
} // namespace cmudb
