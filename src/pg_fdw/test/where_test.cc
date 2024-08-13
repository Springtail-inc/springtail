#include <gtest/gtest.h>

#include <common/common.hh>
#include <common/json.hh>
#include <common/properties.hh>
#include <storage/table.hh>

#include <pg_fdw/pg_fdw_mgr.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

namespace {

    /**
     * Framework for Table and MutableTable testing.
     */
    class FDWWhere_Test : public testing::Test {
    protected:

        // Called once per testsuite.  Create a table and populate it with data
        static void SetUpTestSuite()
        {
            springtail_init();

            auto json = Properties::get(Properties::STORAGE_CONFIG);
            Json::get_to<std::filesystem::path>(json, "table_dir", _table_dir);
            _table_dir = Properties::make_absolute_path(_table_dir);
            std::filesystem::remove_all(_table_dir);

            _base_dir = std::filesystem::temp_directory_path() / "test_fdw_table";
            std::filesystem::remove_all(_base_dir);

            std::filesystem::create_directories(_table_dir);
            std::filesystem::create_directories(_base_dir / std::to_string(_tid));

            _columns = {
                {"col1", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 1, 0, false, true, false},
                {"col2", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 2, 1, false, true, false},
                {"col3", static_cast<uint8_t>(SchemaType::INT32), INT4OID, std::nullopt, 3, 2, false, true, false},
            };

            _data = {
                {1, 1, 1},
                {1, 2, 2},
                {1, 3, 1},
                {1, 3, 2},
                {1, 5, 1},
                {1, 5, 2},
                {2, 1, 1},
                {2, 2, 2},
                {2, 3, 1},
                {2, 3, 2},
                {2, 5, 1},
                {2, 5, 2},
                {3, 1, 1},
                {3, 2, 2},
                {3, 3, 1},
                {3, 3, 2},
                {5, 2, 1},
                {5, 4, 2}
            };

            uint64_t access_xid = 1, target_xid = 2;
            _tid = 1000;

            // create the table via the table mgr
            _create_table(_tid, access_xid);

            access_xid++;
            target_xid++;

            // create a mutable table
            auto mtable = TableMgr::get_instance()->get_mutable_table(_tid, access_xid, target_xid, false);

            // insert a number of rows
            _populate_table(mtable, target_xid);

            // finalize the empty table
            mtable->finalize();

            _table_xid = target_xid+1;
        }

        // Called once per testsuite.  Remove the table directories
        static void TearDownTestSuite() {
            // remove any files created during the run
            std::filesystem::remove_all(_table_dir);
            std::filesystem::remove_all(_base_dir);
        }

        // Pre test setup
        void SetUp() override {
            // setup the attributes to fetch (all columns)
            _attrs = new Form_pg_attribute[_columns.size()];
            for (int i = 0; i < _columns.size(); i++) {
                _attrs[i] = new FormData_pg_attribute();
                _attrs[i]->atttypid = INT4OID;
                _attrs[i]->attnum = _columns[i].position;
                strncpy(_attrs[i]->attname.data, _columns[i].column_name.c_str(), NAMEDATALEN);

                _target_list = lappend(_target_list, makeInteger(_attrs[i]->attnum));
            }
        }

        // Per test teardown
        void TearDown() override {
            if (_attrs != nullptr) {
                for (int i = 0; i < _columns.size(); i++) {
                    if (_attrs[i] != nullptr) {
                        delete _attrs[i];
                    }
                }
                delete[] _attrs;
            }
        }

        inline static uint64_t _tid;
        inline static uint64_t _table_xid;

        inline static std::filesystem::path _table_dir;
        inline static std::filesystem::path _base_dir;

        inline static std::vector<PgMsgSchemaColumn> _columns;

        inline static std::vector<std::vector<int32_t>> _data;

        List *_target_list = nullptr;

        std::map<QualOpName, std::string> _op_names = {
                {EQUALS, "="},
                {NOT_EQUALS, "<>"},
                {LESS_THAN, "<"},
                {LESS_THAN_EQUALS, "<="},
                {GREATER_THAN, ">"},
                {GREATER_THAN_EQUALS, ">="},
        };

        Form_pg_attribute *_attrs;

        static void
        _create_table(uint64_t table_id, uint64_t xid)
        {
            // create a table
            PgMsgTable create_msg;
            create_msg.lsn = 0;
            create_msg.oid = table_id;
            create_msg.xid = xid;
            create_msg.schema = "public";
            create_msg.table = "test_table";
            create_msg.columns = _columns;

            TableMgr::get_instance()->create_table({ xid, 0 }, create_msg);
        }

        std::shared_ptr<Tuple>
        _create_key(const std::string &name)
        {
            auto k = std::make_shared<ConstTypeField<std::string>>(name);
            std::vector<ConstFieldPtr> v({ k });
            return std::make_shared<ValueTuple>(v);
        }

        static std::shared_ptr<Tuple>
        _create_value(const std::vector<int32_t> &data)
        {
            std::vector<ConstFieldPtr> v;

            for (auto &d : data) {
                v.push_back(std::make_shared<ConstTypeField<int32_t>>(d));
            }

            return std::make_shared<ValueTuple>(v);
        }

        static void
        _populate_table(MutableTablePtr mtable, uint64_t xid)
        {
            // insert data to the tree
            for (int i = 0; i < _data.size(); i++) {
                mtable->insert(_create_value(_data[i]), xid, constant::UNKNOWN_EXTENT);
            }
        }

        List *
        _add_qual(int attno, QualOpName op, int32_t val, List *qual_list = nullptr)
        {
            ConstQualPtr qual = new ConstQual();

            qual->base.op = op;
            qual->base.varattno = attno;
            qual->base.typeoid = INT4OID;
            qual->base.opname = const_cast<char *>(_op_names[op].c_str());
            qual->base.right_type = T_Const;
            qual->base.isArray = false;
            qual->base.useOr = false;

            qual->value = Int32GetDatum(val);
            qual->isnull = false;

            return lappend(qual_list, qual);
        }

        std::vector<std::vector<int32_t>>
        _filter_data(List *qual_list)
        {
            std::vector<std::vector<int32_t>> filtered_data;

            for (int i = 0; i < _data.size(); i++) {
                bool row_valid = true;
                ListCell *lc;

                foreach(lc, qual_list) {
                    ConstQualPtr qual = static_cast<ConstQualPtr>(lfirst(lc));
                    int32_t value = DatumGetInt32(qual->value);
                    int32_t data = _data[i][qual->base.varattno - 1];
                    bool valid;

                    switch (qual->base.op) {
                        case EQUALS:
                            valid = data == value;
                            break;
                        case NOT_EQUALS:
                            valid = data != value;
                            break;
                        case LESS_THAN:
                            valid = data < value;
                            break;
                        case LESS_THAN_EQUALS:
                            valid = data <= value;
                            break;
                        case GREATER_THAN:
                            valid = data > value;
                            break;
                        case GREATER_THAN_EQUALS:
                            valid = data >= value;
                            break;
                        default:
                            break;
                    }
                    row_valid &= valid;
                }

                if (row_valid) {
                    filtered_data.push_back(_data[i]);
                }
            }

            return filtered_data;
        }

        void
        _dump_filtered_data(List *quals, const std::vector<std::vector<int32_t>> &filtered_data)
        {
            ListCell *lc;
            foreach(lc, quals) {
                ConstQualPtr qual = static_cast<ConstQualPtr>(lfirst(lc));
                std::cout << "Column: " << qual->base.varattno << " " << qual->base.opname << " Value: " << DatumGetInt32(qual->value) << std::endl;
            }

            for (int i = 0; i < filtered_data.size(); i++) {
                for (int j = 0; j < filtered_data[i].size(); j++) {
                    std::cout << filtered_data[i][j] << " ";
                }
                std::cout << std::endl;
            }
        }

        void
        _run_scan(List *qual_list, const std::vector<std::vector<int32_t>> &filtered_data)
        {
            // get the fdw mgr
            PgFdwMgr *mgr = PgFdwMgr::get_instance();

            // don't call create state as it calls xid mgr, just create state
            auto table = TableMgr::get_instance()->get_table(_tid, _table_xid, constant::MAX_LSN);
            PgFdwState *state = new PgFdwState{table, _tid, _table_xid};

            // begin the scan
            mgr->fdw_begin_scan(state, _target_list, qual_list, nullptr);

            int rows_valid = 0;
            bool eos = false;
            bool row_valid = false;

            // iterate and retrieve full table
            while (!eos) {
                Datum values[_columns.size()];
                bool nulls[_columns.size()];

                row_valid = mgr->fdw_iterate_scan(state, _columns.size(), _attrs, values, nulls, &eos);
                if (eos) {
                    break;
                }

                if (row_valid) {
                    for (int i = 0; i < _columns.size(); i++) {
                        ASSERT_FALSE(nulls[i]);
                        ASSERT_EQ(filtered_data[rows_valid][i], DatumGetInt32(values[i]));
                    }
                    rows_valid++;
                }
            }

            mgr->fdw_end_scan(state); // end the scan, frees state

            ASSERT_EQ(rows_valid, filtered_data.size());
        }

    };

    TEST_F(FDWWhere_Test, Test_FullScan)
    {
        _run_scan(nullptr, _data);

        // col2 = 2; results in a full scan
        List *qual_list = _add_qual(_columns[1].position, EQUALS, 2);
        _run_scan(qual_list, _data);
    }

    TEST_F(FDWWhere_Test, Test_Pkey1)
    {
        // col1 = 2
        List *qual_list = _add_qual(_columns[0].position, EQUALS, 2);
        std::vector<std::vector<int32_t>> filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 > 2
        qual_list = _add_qual(_columns[0].position, GREATER_THAN, 2);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 < 2
        qual_list = _add_qual(_columns[0].position, LESS_THAN, 2);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 >= 2
        qual_list = _add_qual(_columns[0].position, GREATER_THAN_EQUALS, 2);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 <= 2
        qual_list = _add_qual(_columns[0].position, LESS_THAN_EQUALS, 2);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 != 2
        qual_list = _add_qual(_columns[0].position, NOT_EQUALS, 2);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);
    }

    TEST_F(FDWWhere_Test, Test_Pkey2a)
    {
        // col1 = 2 AND col2 = 3
        List *qual_list = _add_qual(_columns[0].position, EQUALS, 2);
        qual_list = _add_qual(_columns[1].position, EQUALS, 3, qual_list);
        std::vector<std::vector<int32_t>> filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 > 2 AND col2 = 3
        qual_list = _add_qual(_columns[0].position, GREATER_THAN, 2);
        qual_list = _add_qual(_columns[1].position, EQUALS, 3, qual_list);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 < 2 AND col2 = 3
        qual_list = _add_qual(_columns[0].position, LESS_THAN, 2);
        qual_list = _add_qual(_columns[1].position, EQUALS, 3, qual_list);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 >= 2 AND col2 = 3
        qual_list = _add_qual(_columns[0].position, GREATER_THAN_EQUALS, 2);
        qual_list = _add_qual(_columns[1].position, EQUALS, 3, qual_list);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 <= 2 AND col2 = 3
        qual_list = _add_qual(_columns[0].position, LESS_THAN_EQUALS, 2);
        qual_list = _add_qual(_columns[1].position, EQUALS, 3, qual_list);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 != 2 AND col2 = 3
        qual_list = _add_qual(_columns[0].position, NOT_EQUALS, 2);
        qual_list = _add_qual(_columns[1].position, EQUALS, 3, qual_list);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);
    }

    TEST_F(FDWWhere_Test, Test_Pkey2b)
    {
        // col1 = 2 AND col2 >= 3
        List *qual_list = _add_qual(_columns[0].position, EQUALS, 2);
        qual_list = _add_qual(_columns[1].position, GREATER_THAN_EQUALS, 3, qual_list);
        std::vector<std::vector<int32_t>> filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 > 2 AND col2 < 7
        qual_list = _add_qual(_columns[0].position, GREATER_THAN, 2);
        qual_list = _add_qual(_columns[1].position, LESS_THAN, 7, qual_list);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 < 9 AND col2 <= 7
        qual_list = _add_qual(_columns[0].position, LESS_THAN, 9);
        qual_list = _add_qual(_columns[1].position, LESS_THAN_EQUALS, 7, qual_list);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 >= 2 AND col2 <> 3
        qual_list = _add_qual(_columns[0].position, GREATER_THAN_EQUALS, 2);
        qual_list = _add_qual(_columns[1].position, NOT_EQUALS, 3, qual_list);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 <= 4 AND col2 = 3
        qual_list = _add_qual(_columns[0].position, LESS_THAN_EQUALS, 4);
        qual_list = _add_qual(_columns[1].position, GREATER_THAN, 3, qual_list);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);
    }

    TEST_F(FDWWhere_Test, Test_Pkey3)
    {
        // col1 = 3 AND col2 = 3 AND col3 = 1
        List *qual_list = _add_qual(_columns[0].position, EQUALS, 3);
        qual_list = _add_qual(_columns[1].position, EQUALS, 3, qual_list);
        qual_list = _add_qual(_columns[2].position, EQUALS, 1, qual_list);
        std::vector<std::vector<int32_t>> filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);

        // col1 > 2 AND col2 = 3 AND col3 = 1
        qual_list = _add_qual(_columns[0].position, GREATER_THAN, 2);
        qual_list = _add_qual(_columns[1].position, EQUALS, 3, qual_list);
        qual_list = _add_qual(_columns[2].position, EQUALS, 1, qual_list);
        filtered_data = _filter_data(qual_list);

        _run_scan(qual_list, filtered_data);
    }
} // namespace
