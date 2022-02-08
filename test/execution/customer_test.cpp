#include <memory>
#include <numeric>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/delete_plan.h"
#include "execution/plans/distinct_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/update_plan.h"
#include "executor_test_util.h"  // NOLINT
#include "gtest/gtest.h"
#include "storage/table/tuple.h"
#include "test_util.h"  // NOLINT
#include "type/value_factory.h"

namespace bustub {

// Parameters for index construction
using KeyType = GenericKey<8>;
using ValueType = RID;
using ComparatorType = GenericComparator<8>;
using HashFunctionType = HashFunction<KeyType>;

// SELECT col_a, col_b FROM test_1 WHERE col_a < 500
TEST_F(ExecutorTest, SimpleSeqScanTest) {
  // Create Values to insert
  std::vector<Value> val11{ValueFactory::GetIntegerValue(100), ValueFactory::GetIntegerValue(10)};
  std::vector<Value> val12{ValueFactory::GetIntegerValue(101), ValueFactory::GetIntegerValue(11)};
  std::vector<Value> val13{ValueFactory::GetIntegerValue(102), ValueFactory::GetIntegerValue(12)};
  std::vector<Value> val14{ValueFactory::GetIntegerValue(100), ValueFactory::GetIntegerValue(13)};
  std::vector<std::vector<Value>> raw_vals_1{val11, val12, val13, val14};

  // Create insert plan node
  auto table_info_1 = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
  InsertPlanNode insert_plan_1{std::move(raw_vals_1), table_info_1->oid_};

  GetExecutionEngine()->Execute(&insert_plan_1, nullptr, GetTxn(), GetExecutorContext());

  // Create Values to insert
  std::vector<Value> val21{ValueFactory::GetIntegerValue(100), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val22{ValueFactory::GetIntegerValue(101), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val23{ValueFactory::GetIntegerValue(102), ValueFactory::GetIntegerValue(22)};
  //   std::vector<Value> val4{ValueFactory::GetIntegerValue(100), ValueFactory::GetIntegerValue(13)};
  std::vector<std::vector<Value>> raw_vals_2{val21, val22, val23};

  // Create insert plan node
  auto table_info_2 = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan_2{std::move(raw_vals_2), table_info_2->oid_};

  GetExecutionEngine()->Execute(&insert_plan_2, nullptr, GetTxn(), GetExecutorContext());

  // Construct sequential scan of table test_4
  const Schema *out_schema1{};
  std::unique_ptr<AbstractPlanNode> scan_plan1{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table3");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema1 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan1 = std::make_unique<SeqScanPlanNode>(out_schema1, nullptr, table_info->oid_);
  }

  // Construct sequential scan of table test_6
  const Schema *out_schema2{};
  std::unique_ptr<AbstractPlanNode> scan_plan2{};
  {
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable("empty_table2");
    auto &schema = table_info->schema_;
    auto *col_a = MakeColumnValueExpression(schema, 0, "colA");
    auto *col_b = MakeColumnValueExpression(schema, 0, "colB");
    out_schema2 = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
    scan_plan2 = std::make_unique<SeqScanPlanNode>(out_schema2, nullptr, table_info->oid_);
  }

  // Construct the join plan
  const Schema *out_schema3{};
  std::unique_ptr<HashJoinPlanNode> join_plan{};
  {
    // Columns from Table 4 have a tuple index of 0 because they are the left side of the join (outer relation)
    auto *table4_col_a = MakeColumnValueExpression(*out_schema1, 0, "colA");
    auto *table4_col_b = MakeColumnValueExpression(*out_schema1, 0, "colB");

    // Columns from Table 6 have a tuple index of 1 because they are the right side of the join (inner relation)
    auto *table6_col_a = MakeColumnValueExpression(*out_schema2, 1, "colA");
    auto *table6_col_b = MakeColumnValueExpression(*out_schema2, 1, "colB");

    out_schema3 = MakeOutputSchema({{"table4_colA", table4_col_a},
                                    {"table4_colB", table4_col_b},
                                    {"table6_colA", table6_col_a},
                                    {"table6_colB", table6_col_b}});

    // Join on table4.colA = table6.colA
    join_plan = std::make_unique<HashJoinPlanNode>(
        out_schema3, std::vector<const AbstractPlanNode *>{scan_plan1.get(), scan_plan2.get()}, table4_col_a,
        table6_col_a);
  }

  std::vector<Tuple> result_set{};
  GetExecutionEngine()->Execute(join_plan.get(), &result_set, GetTxn(), GetExecutorContext());
  ASSERT_EQ(result_set.size(), 4);

  for (const auto &tuple : result_set) {
    const auto t4_col_a = tuple.GetValue(out_schema3, 0).GetAs<int64_t>();
    const auto t4_col_b = tuple.GetValue(out_schema3, 1).GetAs<int32_t>();
    const auto t6_col_a = tuple.GetValue(out_schema3, 2).GetAs<int64_t>();
    const auto t6_col_b = tuple.GetValue(out_schema3, 3).GetAs<int32_t>();

    // Join keys should be equiavlent
    ASSERT_EQ(t4_col_a, t6_col_a);

    // In case of Table 4 and Table 6, corresponding columns also equal
    ASSERT_LT(t4_col_b, TEST4_SIZE);
    ASSERT_LT(t6_col_b, TEST6_SIZE);
    // ASSERT_EQ(t4_col_b, t6_col_b);
  }
}

}  // namespace bustub
