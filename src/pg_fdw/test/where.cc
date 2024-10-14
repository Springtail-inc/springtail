#include <iostream>

#include <fmt/format.h>

#include <common/common.hh>

#include <storage/field.hh>
#include <storage/extent.hh>

#include <sys_tbl_mgr/table.hh>
#include <sys_tbl_mgr/table_mgr.hh>

using namespace springtail;

enum QualOpName {
    UNSUPPORTED,
    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS,
    GREATER_THAN,
    GREATER_THAN_EQUALS,
};

bool check_row(const std::any &row, FieldArrayPtr fields, FieldPtr key_field, QualOpName op)
{
    FieldPtr val_field = fields->at(0);

    switch (op) {
        case EQUALS:
            return key_field->equal(nullptr, val_field, row);
        case NOT_EQUALS:
            return !key_field->equal(nullptr, val_field, row);
        case LESS_THAN:
            return val_field->less_than(row, key_field, nullptr);
        case LESS_THAN_EQUALS:
            return (val_field->less_than(row, key_field, nullptr) ||
                    key_field->equal(nullptr, val_field, row));
        case GREATER_THAN:
            return (!val_field->less_than(row, key_field, nullptr) &&
                    !key_field->equal(nullptr, val_field, row));
        case GREATER_THAN_EQUALS:
            return !val_field->less_than(row, key_field, nullptr);
        default:
            return false;
    }
}

void search(uint64_t db_id, int val, QualOpName op)
{
    TablePtr table = TableMgr::get_instance()->get_table(db_id, 57455, 8);
    std::optional<Table::Iterator> iter_start = std::nullopt;
    std::optional<Table::Iterator> iter_end = std::nullopt;

    FieldArrayPtr key_fields = std::make_shared<FieldArray>(1);
    key_fields->at(0) = std::make_shared<ConstTypeField<int32_t>>(val);
    FieldTuplePtr tuple = std::make_shared<FieldTuple>(key_fields, nullptr);

    if (op == LESS_THAN || op == LESS_THAN_EQUALS || op == NOT_EQUALS) {
        iter_start.emplace(Table::Iterator(table->begin()));
    } else if (op == GREATER_THAN_EQUALS || op == EQUALS) {
        iter_start.emplace(Table::Iterator(table->lower_bound(tuple)));
    } else if (op == GREATER_THAN) {
        iter_start.emplace(Table::Iterator(table->upper_bound(tuple)));
    }

    if (op == LESS_THAN || op == NOT_EQUALS) {
        iter_end.emplace(Table::Iterator(table->lower_bound(tuple)));
    } else if (op == LESS_THAN_EQUALS || op == EQUALS) {
        iter_end.emplace(Table::Iterator(table->upper_bound(tuple)));
    } else if (op == GREATER_THAN || op == GREATER_THAN_EQUALS) {
        iter_end.emplace(Table::Iterator(table->end()));
    }

    FieldPtr key_field = std::make_shared<ConstTypeField<int32_t>>(val);
    auto fields = table->extent_schema()->get_fields();
    std::cout << "Up\n";
    while (*iter_start != *iter_end) {
        auto row = *(*iter_start);
        if (check_row(row, fields, key_field, op)) {
            std::cout << fmt::format("Row: {} {} {}\n", fields->at(0)->get_int32(row), fields->at(1)->get_int32(row), fields->at(2)->get_text(row));
        } else {
            std::cout << fmt::format("Skipping row: {}\n", fields->at(0)->get_int32(row));
        }

        ++(*iter_start);
    }

    if (op == NOT_EQUALS) {
        iter_start.emplace(Table::Iterator(table->upper_bound(tuple)));
        iter_end.emplace(Table::Iterator(table->end()));

        // do it again
        while (*iter_start != *iter_end) {
            auto row = *(*iter_start);
            if (check_row(row, fields, key_field, op)) {
                std::cout << fmt::format("Row: {} {} {}\n", fields->at(0)->get_int32(row), fields->at(1)->get_int32(row), fields->at(2)->get_text(row));
            } else {
                std::cout << fmt::format("Skipping row: {}\n", fields->at(0)->get_int32(row));
            }

            ++(*iter_start);
        }
    }
}


int main(int argc, char **argv)
{
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <val> <direction>\n";
        return 1;
    }

    int val = std::stoi(argv[1]);
    char *direction = (argv[2]);

    std::cout << "searching for: " << val << " op: " << direction << std::endl;

    springtail_init(std::nullopt, std::nullopt, 0);

    // convert operator to enum
    QualOpName op;
    if (strcmp(direction, "lt") == 0) {
        op = LESS_THAN;
    } else if (strcmp(direction, "lte") == 0) {
        op = LESS_THAN_EQUALS;
    } else if (strcmp(direction, "gt") == 0) {
        op = GREATER_THAN;
    } else if (strcmp(direction, "gte") == 0) {
        op = GREATER_THAN_EQUALS;
    } else if (strcmp(direction, "eq") == 0) {
        op = EQUALS;
    } else if (strcmp(direction, "ne") == 0) {
        op = NOT_EQUALS;
    } else {
        std::cerr << "Invalid direction: " << direction << std::endl;
        return 1;
    }

    uint64_t db_id = 1;
    search(db_id, val, op);

    return 0;
}
