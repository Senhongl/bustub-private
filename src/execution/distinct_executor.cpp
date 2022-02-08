//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  LOG_DEBUG("distinct init");
  child_executor_->Init();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    DistinctKey key;
    for (size_t i = 0; i < GetOutputSchema()->GetColumnCount(); i++) {
      key.group_bys_.push_back(tuple->GetValue(GetOutputSchema(), i));
    }
    if (hs_.count(key) == 0) {
      hs_.insert(key);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
