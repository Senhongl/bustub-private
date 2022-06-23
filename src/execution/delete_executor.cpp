//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  txn_ = exec_ctx_->GetTransaction();
  lock_mgr_ = exec_ctx_->GetLockManager();
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  TableHeap *table_heap = table_info_->table_.get();
  std::vector<IndexInfo *> indexes = catalog_->GetTableIndexes(table_info_->name_);

  while (child_executor_->Next(tuple, rid)) {
    // if (!lock_mgr_->LockUpgrade(txn_, *rid)) {
    //   lock_mgr_->LockExclusive(txn_, *rid);
    // }
    if (!table_heap->MarkDelete(*rid, txn_)) {
      // lock_mgr_->Unlock(txn_, *rid);
      continue;
    }
    for (auto index_info : indexes) {
      const auto column_idx = index_info->index_->GetKeyAttrs();
      const auto index_key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, column_idx);
      index_info->index_->DeleteEntry(index_key, *rid, txn_);
      // const IndexWriteRecord delete_record(*rid, plan_->TableOid(), WType::DELETE, *tuple, index_info->index_oid_,
      //                                      catalog_);
      // txn_->AppendTableWriteRecord(delete_record);
    }
    return true;
  }

  return false;
}

}  // namespace bustub
