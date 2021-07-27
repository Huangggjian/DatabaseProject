/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb
{

    INDEX_TEMPLATE_ARGUMENTS
    BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                              BufferPoolManager *buffer_pool_manager,
                              const KeyComparator &comparator,
                              page_id_t root_page_id)
        : index_name_(name), root_page_id_(root_page_id),
          buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

    /*
 * Helper function to decide whether current b+tree is empty
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

    template <typename KeyType, typename ValueType, typename KeyComparator>
    thread_local int BPlusTree<KeyType, ValueType, KeyComparator>::rootLockedCnt = 0;
    /*****************************************************************************
 * SEARCH
 *****************************************************************************/
    /*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                                  std::vector<ValueType> &result,
                                  Transaction *transaction)
    {
        //step 1. find page         找到leaf page
        B_PLUS_TREE_LEAF_PAGE_TYPE *tar = FindLeafPage(key, false, OpType::READ, transaction);
        if (tar == nullptr)
            return false;
        //step 2. find value
        result.resize(1);
        auto ret = tar->Lookup(key, result[0], comparator_); //存下结果到result[0]
        //step 3. unPin buffer pool
        FreePagesInTransaction(false, transaction, tar->GetPageId()); //解锁最后处于lock状态的leaf page (只读状态，共享锁)
        //buffer_pool_manager_->UnpinPage(tar->GetPageId(), false);
        //assert(buffer_pool_manager_->CheckAllUnpined());
        return ret;
    }

    /*****************************************************************************
 * INSERTION
 *****************************************************************************/
    /*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                Transaction *transaction)
    {
        //LockRootPageId(true); //必然是排他锁
        if (IsEmpty())
        {
            LockRootPageId(true);     //必然是排他锁
            StartNewTree(key, value); //空树，则新建page,并且这一页为root page
            TryUnlockRootPageId(true);
            return true;
        }
        //TryUnlockRootPageId(true);
        bool res = InsertIntoLeaf(key, value, transaction);
        //assert(Check());
        return res;
    }
    /*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value)
    {
        //step 1. ask for new page from buffer pool manager
        page_id_t newPageId;
        Page *rootPage = buffer_pool_manager_->NewPage(newPageId);
        assert(rootPage != nullptr);

        B_PLUS_TREE_LEAF_PAGE_TYPE *root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(rootPage->GetData());

        //step 2. update b+ tree's root page id
        root->Init(newPageId, INVALID_PAGE_ID);
        root_page_id_ = newPageId;
        UpdateRootPageId(true);
        //step 3. insert entry directly into leaf page.
        root->Insert(key, value, comparator_);

        buffer_pool_manager_->UnpinPage(newPageId, true);
    }

    /*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                        Transaction *transaction)
    {
        B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage = FindLeafPage(key, false, OpType::INSERT, transaction); //找到该key对应的leaf page
        ValueType v;
        bool exist = leafPage->Lookup(key, v, comparator_); //该key是否已经存在了
        if (exist)                                          //已经存在了，返回false
        {
            //buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
            FreePagesInTransaction(true, transaction); //unlock + unpin最终的leaf page
            return false;
        }
        leafPage->Insert(key, value, comparator_);
        if (leafPage->GetSize() > leafPage->GetMaxSize())                                //大于maxsize了
        {                                                                                //需要split
            B_PLUS_TREE_LEAF_PAGE_TYPE *newLeafPage = Split(leafPage, transaction);      //得到新的leaf, pin=1.unpin这个newleafpage在下下下下行处（unpin pageset）
            InsertIntoParent(leafPage, newLeafPage->KeyAt(0), newLeafPage, transaction); //split后要往父page插入key.插的是新leaf页的第一个key
        }
        //buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
        FreePagesInTransaction(true, transaction);
        return true;
    }

    /*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction)
    {
        //step 1 ask for new page from buffer pool manager
        page_id_t newPageId;
        Page *const newPage = buffer_pool_manager_->NewPage(newPageId); //新建一个页 pin_count=1
        assert(newPage != nullptr);
        newPage->WLatch();                    //写锁加上！
        transaction->AddIntoPageSet(newPage); //该transaction的page set加上这个新的page
        //step 2 move half of key & value pairs from input page to newly created page
        N *newNode = reinterpret_cast<N *>(newPage->GetData()); //new node是指针，指向刚刚新建的page
        newNode->Init(newPageId, node->GetParentPageId());
        node->MoveHalfTo(newNode, buffer_pool_manager_); //旧page后半部分移到新的页
        //fetch page and new page need to unpin page(do it outside)
        return newNode;
    }

    /*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                          const KeyType &key,
                                          BPlusTreePage *new_node,
                                          Transaction *transaction)
    {
        if (old_node->IsRootPage()) //如果旧的page(被split的page)本来就是root page
        {
            //需要新建一个page作为root page
            Page *const newPage = buffer_pool_manager_->NewPage(root_page_id_);
            assert(newPage != nullptr);
            assert(newPage->GetPinCount() == 1);
            //newroot作为指针指向newpage
            B_PLUS_TREE_INTERNAL_PAGE *newRoot = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(newPage->GetData());
            //设置root page的page id
            newRoot->Init(root_page_id_); //root page的parent_id默认INVALID_PAGE_ID
            //root page的array[0].second(value,就是指针)指向old page(被分裂的),  array[1].second指向new page(就是split后新的)
            newRoot->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
            //设置原先两个page的parent page
            old_node->SetParentPageId(root_page_id_);
            new_node->SetParentPageId(root_page_id_);
            //更新root page id
            UpdateRootPageId();
            //fetch page and new page need to unpin page
            //buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
            buffer_pool_manager_->UnpinPage(newRoot->GetPageId(), true); //和第一行的newpage函数对应
            return;
        }
        //得到old page的父page
        page_id_t parentId = old_node->GetParentPageId();
        //获取该parent page
        auto *page = FetchPage(parentId);
        assert(page != nullptr);
        //parent指向parent page
        B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page);
        //设置分裂出来的page的parent page id
        new_node->SetParentPageId(parentId);
        //buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
        //parent page要新增key
        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId()); //在父page的value中：old_node的pageid（属于value）后一个value槽插入new_node的page id
        if (parent->GetSize() > parent->GetMaxSize())
        {
            //parent如果又超过maxsize了，则递归本函数继续向上分裂
            B_PLUS_TREE_INTERNAL_PAGE *newLeafPage = Split(parent, transaction); //new page need unpin
            //递归调用！
            InsertIntoParent(parent, newLeafPage->KeyAt(0), newLeafPage, transaction);
        }
        buffer_pool_manager_->UnpinPage(parentId, true);
    }

    /*****************************************************************************
 * REMOVE
 *****************************************************************************/
    /*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction)
    {
        if (IsEmpty()) //当前树是空树直接返回
            return;
        B_PLUS_TREE_LEAF_PAGE_TYPE *delTar = FindLeafPage(key, false, OpType::DELETE, transaction); //找到要删除的key所在的leaf page
        int curSize = delTar->RemoveAndDeleteRecord(key, comparator_);                              //删除后还剩的size
        if (curSize < delTar->GetMinSize())                                                         //小于最小size则要进行变换
        {
            CoalesceOrRedistribute(delTar, transaction); //合并 or 重分配
        }
        FreePagesInTransaction(true, transaction); //true: 独占！
        //assert(Check());
    }

    /*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction)
    {
        //if (N is the root and N has only one remaining child)
        if (node->IsRootPage()) //如果要调整的是root page（因为本函数自底向上合并，所以有可能会对root page操作）
        {
            bool delOldRoot = AdjustRoot(node); //make the child of N the new root of the tree and delete N
            if (delOldRoot)
            {
                transaction->AddIntoDeletedPageSet(node->GetPageId());
            }
            return delOldRoot;
        }
        //Let N2 be the previous or next child of parent(N)
        N *node2;
        bool isRightSib = FindLeftSibling(node, node2, transaction); //是否右兄弟借。默认往左边借。 node2是兄弟节点
        BPlusTreePage *parent = FetchPage(node->GetParentPageId());
        B_PLUS_TREE_INTERNAL_PAGE *parentPage = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parent);
        //if (entries in N and N2 can fit in a single node)
        //可以合并走合并（Coalesce），不能合并走Redistribute
        if (node->GetSize() + node2->GetSize() <= node->GetMaxSize())
        {
            if (isRightSib)
            {
                //??? 应该调用的是std::swap吧。交换2个指针指向的页
                swap(node, node2); //交换。保证node is after node2
            }                      //assumption node is after node2
            int removeIndex = parentPage->ValueIndex(node->GetPageId());
            //node要移到node2。且在他们的parent page中要删掉node的index(removeIndex)
            //node（后）移到node2 (前)  全部移动，因为可以全部移动
            Coalesce(node2, node, parentPage, removeIndex, transaction); //unpin node,node2
            buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
            return true;
        }
        /* Redistribution: borrow an entry from N2 */
        int nodeInParentIndex = parentPage->ValueIndex(node->GetPageId());
        //node（后）移到node2 (前)  //不能全部移动（那就移动一个）
        Redistribute(node2, node, nodeInParentIndex); //unpin node,node2 in this func  //parent page并不会少一项key，不需要递归
        buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), false);
        return false;
    }

    //FIND SIBLING 就是找到前一个兄弟，只有当自己是头节点的时候，才找后一个同时返回TRUE。
    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    bool BPLUSTREE_TYPE::FindLeftSibling(N *node, N *&sibling, Transaction *transaction)
    {
        auto page = FetchPage(node->GetParentPageId());                                          //当前page的parent page
        B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page); //pointer
        int index = parent->ValueIndex(node->GetPageId());                                       //获取当前页处于parent page所有child page中的哪个位置（pointer）
        int siblingIndex = index - 1;                                                            //默认是往左兄弟借节点
        if (index == 0)                                                                          //如果当前页属于parent page所有child page中最左边的
        {                                                                                        //no left sibling
            siblingIndex = index + 1;                                                            //只能往右兄弟借节点
        }
        sibling = reinterpret_cast<N *>(CrabingProtocalFetchPage( //兄弟 sibling page
            parent->ValueAt(siblingIndex), OpType::DELETE, -1, transaction));
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
        return index == 0; //index == 0 means sibling is right  返回true代表找的右兄弟，否则左兄弟
    }

    /*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
    //合并的方法，就是把后面的移到前面的。随后把全部移走的那个节点从PAGE TABLE里删了。然后移除PARENT的节点。最后按PARENT是不是小于阈值，递归。
    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    bool BPLUSTREE_TYPE::Coalesce(
        N *&neighbor_node, N *&node,
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
        int index, Transaction *transaction)
    {
        //assumption neighbor_node is before node
        assert(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize());
        //move later one to previous one
        node->MoveAllTo(neighbor_node, index, buffer_pool_manager_); //node移到neighboor那里去了
        transaction->AddIntoDeletedPageSet(node->GetPageId());       //这一页删掉了
        parent->Remove(index);                                       //移除PARENT的节点
        // 因为走到这个分支的都是INTERNAL PAGE。
        // LEAF PAGE 是不用小于等于的，而是直接小于。
        // 原因就是INTERNAL PAGE，最小的SIZE就是2.我们设想MAXSIZE 是4的INTERNAL PAGE。 如果SIZE 是2，那么有效的节点其实只有1个（因为第一个是INVALID KEY），所以等于的情况也是需要做合并的。

        // 如果MAXSIZE是5，SIZE 是3，有效节点是2 的情况可以允许。所以有了这里的小于等于

        if (parent->GetSize() <= parent->GetMinSize())
        {
            return CoalesceOrRedistribute(parent, transaction); //递归调用  （因为parent page删掉了一个key，所以可能会<=minsize,需要继续合并）
        }
        return false;
    }

    /*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) //前 后
    {
        if (index == 0)
        {
            neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_); //兄弟是右节点的情况。则把右边的第一个放到左边最后
        }
        else
        {
            neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_); //左兄弟的最后一个值拿过来放右边第一个
        }
    }
    /*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node)
    {
        //意味着整棵树的删除
        if (old_root_node->IsLeafPage())
        { // case 2:  when you delete the last element in whole b+ tree
            assert(old_root_node->GetSize() == 0);
            assert(old_root_node->GetParentPageId() == INVALID_PAGE_ID);
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId();
            return true; //返回true, 告诉调用它的函数，把old_root_node删掉
        }
        // 是internal PAGE，但是size==1了。就意味他的唯一的孩子 需要继承来做ROOT PAGE。
        if (old_root_node->GetSize() == 1)
        { // case 1: when you delete the last element in root page, but root page still has one last child
            //root page的孩子就剩下一个了（https://upload-images.jianshu.io/upload_images/10803273-cfbcd38a10410d69.png?imageMogr2/auto-orient/strip|imageView2/2/w/758/format/webp）
            B_PLUS_TREE_INTERNAL_PAGE *root = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(old_root_node);
            const page_id_t newRootId = root->RemoveAndReturnOnlyChild();
            root_page_id_ = newRootId;
            UpdateRootPageId();
            // set the new root's parent id "INVALID_PAGE_ID"
            Page *page = buffer_pool_manager_->FetchPage(newRootId);
            assert(page != nullptr);
            B_PLUS_TREE_INTERNAL_PAGE *newRoot = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
            newRoot->SetParentPageId(INVALID_PAGE_ID);
            buffer_pool_manager_->UnpinPage(newRootId, true);
            return true; //返回true, 告诉调用它的函数，把old_root_node删掉
        }
        //其他情况无需调整（root page, minsize > size >= 2没事）
        return false;
    }

    /*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
    /*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin()//leaf page最左边的key&value是begin迭代器
    {
        KeyType useless;
        auto start_leaf = FindLeafPage(useless, true);//一直往左边走，找到所有leaf page中最左边的page
        TryUnlockRootPageId(false);//root page 解锁。因为上一句话里对root page锁着了
        return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);//是个IndexIterator类
    }

    /*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key)//leaf page中找到key为参数指定的key，作为begin迭代器
    {
        auto start_leaf = FindLeafPage(key);
        TryUnlockRootPageId(false);
        if (start_leaf == nullptr)//空树？
        {
            return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
        }
        int idx = start_leaf->KeyIndex(key, comparator_);
        return INDEXITERATOR_TYPE(start_leaf, idx, buffer_pool_manager_);
    }

    /*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
    /*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
    //一开始没明白为啥需要一个LEFT MOST，后来知道，在实现ITERATOR的时候，需要先定位到最左的叶子节点，就是这个作用的。
    INDEX_TEMPLATE_ARGUMENTS
    B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                             bool leftMost, OpType op,
                                                             Transaction *transaction)
    {
        bool exclusive = (op != OpType::READ);
        LockRootPageId(exclusive); //上锁
        if (IsEmpty())
        {
            TryUnlockRootPageId(exclusive);
            return nullptr;
        }
        // you need to first fetch the page from buffer pool using its unique page_id, then reinterpret cast to either
        // a leaf or an internal page, and unpin the page after any writing or reading operations.
        BPlusTreePage *pointer = CrabingProtocalFetchPage(root_page_id_, op, -1, transaction); //获取指向root_page的指针
        page_id_t next;
        page_id_t cur = root_page_id_; //next存的是当前page的child page id(pointer)
        //从root出发，一直找到leaf为止
        while (!pointer->IsLeafPage())
        //前向更新pointer和cur(pointer指向本次page, next是本轮pointer要去的child page, cur是上一轮的page pointer去的child page)
        {
            B_PLUS_TREE_INTERNAL_PAGE *internalPage = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(pointer);
            //next：当前internal页的child page
            if (leftMost)
            {
                next = internalPage->ValueAt(0); //一直找最小页，后面根据迭代器来找参数key?
            }
            else
            {
                next = internalPage->Lookup(key, comparator_); //根据参数key来找到key所在的leaf page
            }
            pointer = CrabingProtocalFetchPage(next, op, cur, transaction);
            cur = next;
        }
        //最终返回的pointer一定是指向leaf page。且只有最终leaf page是处于lock状态的
        return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(pointer);
    }
    INDEX_TEMPLATE_ARGUMENTS
    BPlusTreePage *BPLUSTREE_TYPE::FetchPage(page_id_t page_id)
    {
        auto page = buffer_pool_manager_->FetchPage(page_id);
        return reinterpret_cast<BPlusTreePage *>(page->GetData());
    }

    INDEX_TEMPLATE_ARGUMENTS
    BPlusTreePage *BPLUSTREE_TYPE::CrabingProtocalFetchPage(page_id_t page_id, OpType op, page_id_t previous, Transaction *transaction)
    {
        bool exclusive = op != OpType::READ;                                //独占锁？
        auto page = buffer_pool_manager_->FetchPage(page_id);               //获取page
        Lock(exclusive, page);                                              //此页上锁
        auto treePage = reinterpret_cast<BPlusTreePage *>(page->GetData()); //指针指向该page（不管是internal page还是leaf page，都转化为BPlusPage）
        if (previous > 0 && (!exclusive || treePage->IsSafe(op)))           //previous == -1代表当前是root page。其他都是>0.  root_page没有previous page 不需要执行这一句
        {
            //其他页的相关操作相关的在这里
            FreePagesInTransaction(exclusive, transaction, previous);
        }
        if (transaction != nullptr)            //其他的页unlock+unpin
            transaction->AddIntoPageSet(page); //同时把这一页要放进set。是lock状态
        return treePage;
    }

    //root page 由class BPlusTree中的类成员RWMutex mutex_;复杂锁定。其他每一页都有自己的RWMutex rwlatch_;实例
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::FreePagesInTransaction(bool exclusive, Transaction *transaction, page_id_t cur) //cur默认-1
    {
        TryUnlockRootPageId(exclusive); //解锁root page
        if (transaction == nullptr)
        {
            assert(!exclusive && cur >= 0);
            Unlock(false, cur);                          //先unlock
            buffer_pool_manager_->UnpinPage(cur, false); //再unpin
            return;
        }
        for (Page *page : *transaction->GetPageSet()) //该transaction有关联的page
        {
            int curPid = page->GetPageId();
            Unlock(exclusive, page);                            //先unlock
            buffer_pool_manager_->UnpinPage(curPid, exclusive); //再unpin
            //该page是不是在deleteset里。是的话要delete
            if (transaction->GetDeletedPageSet()->find(curPid) != transaction->GetDeletedPageSet()->end())
            {
                buffer_pool_manager_->DeletePage(curPid);
                transaction->GetDeletedPageSet()->erase(curPid);
            }
        }
        assert(transaction->GetDeletedPageSet()->empty());
        transaction->GetPageSet()->clear();
    }

    /*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) //默认参数是0
    {
        HeaderPage *header_page = static_cast<HeaderPage *>(
            buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
        if (insert_record)
            // create a new record<index_name + root_page_id> in header_page
            header_page->InsertRecord(index_name_, root_page_id_);
        else
            // update root_page_id in header_page
            header_page->UpdateRecord(index_name_, root_page_id_);
        buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true); //fetchpage ==> unpinpage
    }

    /*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
    INDEX_TEMPLATE_ARGUMENTS
    std::string BPLUSTREE_TYPE::ToString(bool verbose)
    {
        if (IsEmpty())
        {
            return "Empty tree";
        }
        std::queue<BPlusTreePage *> todo, tmp;
        std::stringstream tree;
        auto node = reinterpret_cast<BPlusTreePage *>(
            buffer_pool_manager_->FetchPage(root_page_id_));
        if (node == nullptr)
        {
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
        }
        todo.push(node);
        bool first = true;
        while (!todo.empty())
        {
            node = todo.front();
            if (first)
            {
                first = false;
                tree << "| ";
            }
            // leaf page, print all key-value pairs
            if (node->IsLeafPage())
            {
                auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
                tree << page->ToString(verbose) << "(" << node->GetPageId() << ")| ";
            }
            else
            {
                auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
                tree << page->ToString(verbose) << "(" << node->GetPageId() << ")| ";
                page->QueueUpChildren(&tmp, buffer_pool_manager_);
            }
            todo.pop();
            if (todo.empty() && !tmp.empty())
            {
                todo.swap(tmp);
                tree << '\n';
                first = true;
            }
            // unpin node when we are done
            buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
        }
        return tree.str();
    }

    /*
 * This method is used for test only
 * Read data from file and insert one by one
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                        Transaction *transaction)
    {
        int64_t key;
        std::ifstream input(file_name);
        while (input)
        {
            input >> key;

            KeyType index_key;
            index_key.SetFromInteger(key);
            RID rid(key);
            Insert(index_key, rid, transaction);
        }
    }
    /*
 * This method is used for test only
 * Read data from file and remove one by one
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                        Transaction *transaction)
    {
        int64_t key;
        std::ifstream input(file_name);
        while (input)
        {
            input >> key;
            KeyType index_key;
            index_key.SetFromInteger(key);
            Remove(index_key, transaction);
        }
    }

    /***************************************************************************
 *  Check integrity of B+ tree data structure.
 ***************************************************************************/

    INDEX_TEMPLATE_ARGUMENTS
    int BPLUSTREE_TYPE::isBalanced(page_id_t pid)
    {
        if (IsEmpty())
            return true;
        auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
        if (node == nullptr)
        {
            throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while isBalanced");
        }
        int ret = 0;
        if (!node->IsLeafPage())
        {
            auto page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(node);
            int last = -2;
            for (int i = 0; i < page->GetSize(); i++)
            {
                int cur = isBalanced(page->ValueAt(i));
                if (cur >= 0 && last == -2)
                {
                    last = cur;
                    ret = last + 1;
                }
                else if (last != cur)
                {
                    ret = -1;
                    break;
                }
            }
        }
        buffer_pool_manager_->UnpinPage(pid, false);
        return ret;
    }

    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::isPageCorr(page_id_t pid, pair<KeyType, KeyType> &out)
    {
        if (IsEmpty())
            return true;
        auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
        if (node == nullptr)
        {
            throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while isPageCorr");
        }
        bool ret = true;
        if (node->IsLeafPage())
        {
            auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
            int size = page->GetSize();
            ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
            for (int i = 1; i < size; i++)
            {
                if (comparator_(page->KeyAt(i - 1), page->KeyAt(i)) > 0)
                {
                    ret = false;
                    break;
                }
            }
            out = pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
        }
        else
        {
            auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
            int size = page->GetSize();
            ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
            pair<KeyType, KeyType> left, right;
            for (int i = 1; i < size; i++)
            {
                if (i == 1)
                {
                    ret = ret && isPageCorr(page->ValueAt(0), left);
                }
                ret = ret && isPageCorr(page->ValueAt(i), right);
                ret = ret && (comparator_(page->KeyAt(i), left.second) > 0 && comparator_(page->KeyAt(i), right.first) <= 0);
                ret = ret && (i == 1 || comparator_(page->KeyAt(i - 1), page->KeyAt(i)) < 0);
                if (!ret)
                    break;
                left = right;
            }
            out = pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
        }
        buffer_pool_manager_->UnpinPage(pid, false);
        return ret;
    }

    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::Check(bool forceCheck)
    {
        if (!forceCheck && !openCheck)
        {
            return true;
        }
        pair<KeyType, KeyType> in;
        bool isPageInOrderAndSizeCorr = isPageCorr(root_page_id_, in);
        bool isBal = (isBalanced(root_page_id_) >= 0);
        bool isAllUnpin = buffer_pool_manager_->CheckAllUnpined();
        if (!isPageInOrderAndSizeCorr)
            cout << "problem in page order or page size" << endl;
        if (!isBal)
            cout << "problem in balance" << endl;
        if (!isAllUnpin)
            cout << "problem in page unpin" << endl;
        return isPageInOrderAndSizeCorr && isBal && isAllUnpin;
    }

    template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
    template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
    template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
    template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
    template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
} // namespace cmudb
