//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  LOG_DEBUG("nested_loop init");
  catalog_ = exec_ctx_->GetCatalog();
  txn_ = exec_ctx_->GetTransaction();
  predicate_ = plan_->Predicate();
  skip_outer_loop_ = false;
  left_executor_->Init();
  right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (skip_outer_loop_ || left_executor_->Next(&outer_tuple_, &outer_rid_)) {
    while (right_executor_->Next(&inner_tuple_, &inner_rid_)) {
      if (predicate_ == nullptr || predicate_
                                       ->EvaluateJoin(&outer_tuple_, plan_->GetLeftPlan()->OutputSchema(),
                                                      &inner_tuple_, plan_->GetRightPlan()->OutputSchema())
                                       .GetAs<bool>()) {
        skip_outer_loop_ = true;
        std::vector<Value> combined_value;
        for (const auto &col : GetOutputSchema()->GetColumns()) {
          combined_value.push_back(col.GetExpr()->EvaluateJoin(&outer_tuple_, left_executor_->GetOutputSchema(),
                                                               &inner_tuple_, right_executor_->GetOutputSchema()));
        }
        *tuple = Tuple(combined_value, GetOutputSchema());
        return true;
      }
    }
    skip_outer_loop_ = false;
    right_executor_->Init();
  }
  return false;
}

}  // namespace bustub
