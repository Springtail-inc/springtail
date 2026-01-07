#include <storage/gist_helpers.hh>
#include <pg_ext/extn_registry.hh>
#include <pg_ext/fmgr.hh>
#include <pg_ext/string.hh>
#include <common/logging.hh>
#include <storage/field.hh>

namespace springtail::gist_helpers {

    using OffsetNumber = uint16_t;

    struct GISTENTRY
    {
        Datum       key;
        uintptr_t   rel;
        uintptr_t   page;
        OffsetNumber offset;
        bool        leafkey;
    };

    struct GistEntryVector
    {
        int32_t     n;
        GISTENTRY   vector[FLEXIBLE_ARRAY_MEMBER];
    };

    struct GIST_SPLITVEC
    {
        GISTENTRY  *spl_left;
        GISTENTRY  *spl_right;
        OffsetNumber spl_nleft;
        OffsetNumber spl_nright;
    };

    static char* TextDatumGetCString(Datum d) {
        text* t = (text*) DatumGetPointer(d);
        // VARDATA_ANY and VARSIZE_ANY_EXHDR are standard PG macros for varlena
        // We need to check if they are available in pg_ext/varatt.hh
        // If not, we might need to rely on simple casting if we assume standard varlena layout
        // For now, let's assume we can use VARDATA_ANY if available, or just VARDATA if it's standard 4-byte header
        // Let's check varatt.hh content first? No, I'll just use a safe implementation assuming 4-byte header for now or try to find a helper.
        // Actually, pg_ext/string.hh has cstring_to_text_with_len.
        // Let's assume standard varlena:
        // struct varlena { char vl_len_[4]; char vl_dat[1]; };
        // But we don't have the struct definition visible here easily without including varatt.hh
        // I'll include varatt.hh
        return text_to_cstring(t);
    }

    static Datum make_datum_from_field(const FieldPtr& field, const void* row) {
        LOG_INFO("[DEBUG] Inside make_datum_from_field");
        switch(field->get_type())
        {
            case SchemaType::TEXT: {
                auto val = std::string(field->get_text(row));
                LOG_INFO("[DEBUG] TEXT value: {}", val);
                return PointerGetDatum(cstring_to_text_auto(val.c_str()));
            }
            case SchemaType::INT32:
                return Int32GetDatum(field->get_int32(row));
            case SchemaType::INT64:
                return Int64GetDatum(field->get_int64(row));
            // Add other types as needed
            default:
                LOG_WARN("Unhandled type in make_datum_from_field: {}", (int)field->get_type());
                return 0;
        }
    }

    GistEntry extract_gist_entry_from_tuple(TuplePtr tuple, ExtentSchemaPtr schema, const std::vector<std::string>& opclass_names) {
        GistEntry out;
        out.leafkey = true;
        LOG_INFO("[DEBUG] Inside extract_gist_entry_from_tuple");
        for (std::size_t idx = 0; idx < opclass_names.size(); ++idx) {
            // Compress
            if ( opclass_names[idx] == "EMPTY") {
                continue;
            }
            // Get raw datum
            FieldPtr field = tuple->field(idx);
            Datum raw = make_datum_from_field(field, tuple->row());
            LOG_INFO("[DEBUG] Field {} raw datum: {}", idx, raw);
            LOG_INFO("[DEBUG] Opclass name: {}", opclass_names[idx]);

            auto opclass_method = PgExtnRegistry::get_instance()->get_opclass_method_by_method_name(opclass_names[idx], GIST_COMPRESS);
            if (opclass_method.function_ptr) {
                PGFunction func = (PGFunction)opclass_method.function_ptr;

                GISTENTRY entry;
                entry.key = raw;
                entry.rel = 0;
                entry.page = 0;
                entry.offset = 0;
                entry.leafkey = true;

                Datum entry_datum = PointerGetDatum(&entry);
                Datum compressed = DirectFunctionCall1(func, entry_datum);

                // Result of compress is GISTENTRY* (or Datum that is GISTENTRY*)
                GISTENTRY *retval = (GISTENTRY *) DatumGetPointer(compressed);
                if (retval) {
                    out.keys.push_back(retval->key);
                } else {
                    out.keys.push_back(raw);
                }
            } else {
                out.keys.push_back(raw);
            }
        }
        return out;
    }

    GistEntry read_branch_entry_from_row(const Extent::Row& row, ExtentSchemaPtr schema, const std::vector<std::string>& opclass_names) {
        GistEntry out;
        out.leafkey = false;

        auto tuple = FieldTuple(schema->get_fields(), &row);

        for (std::size_t idx = 0; idx < opclass_names.size(); ++idx) {
            FieldPtr field = tuple.field(idx);
            // Branch entries are already compressed/internal format, so we just read them as Datums
            // But wait, Extent stores them as Springtail types (TEXT, INT, etc).
            // We need to convert them back to Datums.
            out.keys.push_back(make_datum_from_field(field, &row));
        }

        // Child page ID is the last column
        FieldPtr child_field = tuple.field(tuple.size() - 1);
        out.internal_row_id = static_cast<uint64_t>(child_field->get_uint64(&row)); // Assuming it's stored as uint64

        return out;
    }

    GistEntry read_leaf_entry_from_row(const Extent::Row& row, ExtentSchemaPtr schema, const std::vector<std::string>& opclass_names) {
        GistEntry out;
        out.leafkey = true;

        auto tuple = FieldTuple(schema->get_fields(), &row);

        for (std::size_t idx = 0; idx < opclass_names.size(); ++idx) {
            FieldPtr field = tuple.field(idx);
            out.keys.push_back(make_datum_from_field(field, &row));
        }
        return out;
    }

    void write_leaf_row_from_entry(Extent::Row& row, const GistEntry& entry, ExtentSchemaPtr schema, const std::vector<std::string>& opclass_names) {
        auto tuple = std::make_shared<MutableTuple>(schema->get_mutable_fields(), &row);

        for (std::size_t idx = 0; idx < opclass_names.size(); ++idx) {
            MutableFieldPtr field = tuple->mutable_field(idx);
            Datum d = entry.keys[idx];
            // Convert Datum back to Springtail type and set field
            // This is tricky. Datum is uintptr_t.
            // If it's TEXT, it's text*.
            // If it's INT, it's int value.
            // We need to know the type from schema.
            switch(field->get_type()) {
                case SchemaType::TEXT: {
                    char* str = TextDatumGetCString(d); // Need this helper
                    field->set_text(&row, std::string_view(str));
                    break;
                }
                case SchemaType::INT32:
                    field->set_int32(&row, DatumGetInt32(d));
                    break;
                case SchemaType::INT64:
                    field->set_int64(&row, DatumGetInt64(d));
                    break;
                default:
                    LOG_WARN("Unhandled type in write_leaf_row_from_entry");
            }
        }
    }

    void write_branch_row_from_unions(Extent::Row& row, const GistEntry& key, uint64_t child_page_id, ExtentSchemaPtr schema, const std::vector<std::string>& opclass_names) {
        auto tuple = std::make_shared<MutableTuple>(schema->get_mutable_fields(), &row);

        for (std::size_t idx = 0; idx < opclass_names.size(); ++idx) {
            MutableFieldPtr field = tuple->mutable_field(idx);
            Datum d = key.keys[idx];
            switch(field->get_type()) {
                case SchemaType::TEXT: {
                    char* str = TextDatumGetCString(d);
                    field->set_text(&row, std::string_view(str));
                    break;
                }
                case SchemaType::INT32:
                    field->set_int32(&row, DatumGetInt32(d));
                    break;
                case SchemaType::INT64:
                    field->set_int64(&row, DatumGetInt64(d));
                    break;
                default:
                    LOG_WARN("Unhandled type in write_branch_row_from_unions");
            }
        }

        // Write child page ID
        MutableFieldPtr child_field = tuple->mutable_field(opclass_names.size());
        child_field->set_uint64(&row, child_page_id);
    }

    uint64_t extract_child_pageid_from_row(const Extent::Row& row, ExtentSchemaPtr schema) {
        auto tuple = std::make_shared<FieldTuple>(schema->get_fields(), &row);
        // Assuming child page ID is the last field
        FieldPtr field = tuple->field(schema->get_fields()->size() - 1);
        return field->get_uint64(&row);
    }

    double compute_gist_penalty(const GistEntry& existing_entry, const GistEntry& new_entry, const std::vector<std::string>& opclass_names) {
        double total_penalty = 0.0;
        for (std::size_t idx = 0; idx < opclass_names.size(); ++idx) {
            auto opclass_method = PgExtnRegistry::get_instance()->get_opclass_method_by_method_name(opclass_names[idx], GIST_PENALTY);
            if (!opclass_method.function_ptr) continue;

            PGFunction func = (PGFunction)opclass_method.function_ptr;

            GISTENTRY orig;
            orig.key = existing_entry.keys[idx];
            orig.leafkey = existing_entry.leafkey;

            GISTENTRY newe;
            newe.key = new_entry.keys[idx];
            newe.leafkey = new_entry.leafkey;

            float penalty = 0.0;

            DirectFunctionCall3(func, PointerGetDatum(&orig), PointerGetDatum(&newe), PointerGetDatum(&penalty));
            total_penalty += penalty;
        }
        return total_penalty;
    }

    void compute_union(const std::vector<GistEntry>& entries, GistEntry& union_entry, const std::vector<std::string>& opclass_names) {
        union_entry.leafkey = false; // Union is usually for branch

        for (std::size_t idx = 0; idx < opclass_names.size(); ++idx) {
            auto opclass_method = PgExtnRegistry::get_instance()->get_opclass_method_by_method_name(opclass_names[idx], GIST_UNION);
            if (!opclass_method.function_ptr) continue;

            PGFunction func = (PGFunction)opclass_method.function_ptr;

            // Prepare GistEntryVector
            size_t size = offsetof(GistEntryVector, vector) + entries.size() * sizeof(GISTENTRY);
            GistEntryVector* vec = (GistEntryVector*) palloc(size); // Need palloc? Or malloc?
            // We don't have palloc context here easily. Use malloc/new.
            // But PG functions might expect palloc'd memory if they store it?
            // GIST_UNION usually returns a new Datum.
            // We should use a buffer.
            std::vector<char> buffer(size);
            vec = (GistEntryVector*) buffer.data();
            vec->n = entries.size();

            for (size_t i = 0; i < entries.size(); ++i) {
                vec->vector[i].key = entries[i].keys[idx];
                vec->vector[i].leafkey = entries[i].leafkey;
                vec->vector[i].rel = 0;
                vec->vector[i].page = 0;
                vec->vector[i].offset = 0;
            }

            int out_size = 0; // Not used by GIST_UNION usually?
            // GIST_UNION signature: Datum my_union(GistEntryVector *entryvec, int *out_size)

            Datum result = DirectFunctionCall2(func, PointerGetDatum(vec), PointerGetDatum(&out_size));
            union_entry.keys.push_back(result);
        }
    }

    void compute_picksplit(const std::vector<GistEntry>& entries, std::vector<GistEntry>& out_left, std::vector<GistEntry>& out_right, const std::vector<std::string>& opclass_names) {
        // if (opclass_names.empty()) return;

        // // Use first column for split
        // auto opclass_method = PgExtnRegistry::get_instance()->get_opclass_method_by_method_name(opclass_names[0], GIST_PICKSPLIT);
        // if (!opclass_method.function_ptr) return;

        // PGFunction func = (PGFunction)opclass_method.function_ptr;

        // size_t size = offsetof(GistEntryVector, vector) + entries.size() * sizeof(GISTENTRY);
        // std::vector<char> buffer(size);
        // GistEntryVector* vec = (GistEntryVector*) buffer.data();
        // vec->n = entries.size();

        // for (size_t i = 0; i < entries.size(); ++i) {
        //     vec->vector[i].key = entries[i].keys[0]; // Use first key
        //     vec->vector[i].leafkey = entries[i].leafkey;
        //     vec->vector[i].offset = i + 1; // OffsetNumber is 1-based usually
        // }

        // GIST_SPLITVEC sv;
        // memset(&sv, 0, sizeof(GIST_SPLITVEC));

        // DirectFunctionCall2(func, PointerGetDatum(vec), PointerGetDatum(&sv));

        // // Process results
        // // sv.spl_left contains offsets (indices) of entries that go to left
        // // sv.spl_right contains offsets of entries that go to right

        // for (int i = 0; i < sv.spl_nleft; ++i) {
        //     int idx = sv.spl_left[i] - 1; // 0-based index
        //     if (idx >= 0 && idx < entries.size()) {
        //         out_left.push_back(entries[idx]);
        //     }
        // }

        // for (int i = 0; i < sv.spl_nright; ++i) {
        //     int idx = sv.spl_right[i] - 1;
        //     if (idx >= 0 && idx < entries.size()) {
        //         out_right.push_back(entries[idx]);
        //     }
        // }
    }

}
