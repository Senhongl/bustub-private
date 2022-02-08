//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  LOG_DEBUG("update init");
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  txn_ = exec_ctx_->GetTransaction();
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  TableHeap *table_heap = table_info_->table_.get();
  std::vector<IndexInfo *> indexes = catalog_->GetTableIndexes(table_info_->name_);
  RID update_rid;
  Tuple old_tuple;
  Tuple new_tuple;

  if (!child_executor_->Next(&old_tuple, &update_rid)) {
    return false;
  }

  new_tuple = GenerateUpdatedTuple(old_tuple);
  if (!table_heap->UpdateTuple(new_tuple, update_rid, txn_)) {
    return false;
  }

  for (auto index_info : indexes) {
    const auto column_idx = index_info->index_->GetKeyAttrs();
    const auto old_index_key =
        old_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(), column_idx);
    const auto new_index_key =
        new_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(), column_idx);
    index_info->index_->DeleteEntry(old_index_key, update_rid, txn_);
    index_info->index_->InsertEntry(new_index_key, update_rid, txn_);
  }

  return true;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
