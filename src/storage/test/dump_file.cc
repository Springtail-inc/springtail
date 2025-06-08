#include <fmt/ranges.h>

#include <common/init.hh>
#include <storage/field.hh>
#include "pg_repl/pg_common.hh"

using namespace springtail;

FieldArrayPtr
generate_fields(ExtentPtr extent)
{
    // first determine the base field offsets
    uint32_t fixed_width = 0;
    for (uint8_t type : extent->header().field_types) {
        SchemaType stype = static_cast<SchemaType>(type & 0x7F);
        switch (stype) {
            case (SchemaType::UINT64):
            case (SchemaType::INT64):
            case (SchemaType::FLOAT64):
                fixed_width += 8;
                break;

            case (SchemaType::TEXT):
            case (SchemaType::BINARY):
            case (SchemaType::NUMERIC):
            case (SchemaType::UINT32):
            case (SchemaType::INT32):
            case (SchemaType::FLOAT32):
                fixed_width += 4;
                break;

            case (SchemaType::UINT16):
            case (SchemaType::INT16):
                fixed_width += 2;
                break;

            case (SchemaType::UINT8):
            case (SchemaType::INT8):
                fixed_width += 1;
                break;

            case (SchemaType::BOOLEAN):
                break;

            default:
                LOG_ERROR("Invalid type: {}", (type & 0x7F));
                exit(-1);
        }
    }

    // construct the fields based on the types in the extent header
    auto fields = std::make_shared<FieldArray>();
    uint32_t byte_pos = 0;
    uint64_t bit_pos = static_cast<uint64_t>(fixed_width) << 3;
    for (uint8_t type : extent->header().field_types) {
        ExtentFieldPtr field;

        bool nullable = (type & 0x80) > 0;
        SchemaType stype = static_cast<SchemaType>(type & 0x7F);
        switch (stype) {
            case (SchemaType::UINT64):
            case (SchemaType::INT64):
            case (SchemaType::FLOAT64):
                field = std::make_shared<ExtentField>(stype, byte_pos);
                if (nullable) {
                    field->allow_null((bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos;
                }
                byte_pos += 8;
                break;

            case (SchemaType::TEXT):
            case (SchemaType::BINARY):
            case (SchemaType::NUMERIC):
            case (SchemaType::UINT32):
            case (SchemaType::INT32):
            case (SchemaType::FLOAT32):
                field = std::make_shared<ExtentField>(stype, byte_pos);
                if (nullable) {
                    field->allow_null((bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos;
                }
                byte_pos += 4;
                break;

            case (SchemaType::UINT16):
            case (SchemaType::INT16):
                field = std::make_shared<ExtentField>(stype, byte_pos);
                if (nullable) {
                    field->allow_null((bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos;
                }
                byte_pos += 2;
                break;

            case (SchemaType::UINT8):
            case (SchemaType::INT8):
                field = std::make_shared<ExtentField>(stype, byte_pos);
                if (nullable) {
                    field->allow_null((bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos;
                }
                byte_pos += 1;
                break;

            case (SchemaType::BOOLEAN):
                uint64_t bool_pos = bit_pos++;
                field = std::make_shared<ExtentField>(stype, (bool_pos >> 3), (bool_pos & 0x7));
                if (nullable) {
                    field->allow_null((bit_pos >> 3), (bit_pos & 0x7));
                    ++bit_pos;
                }
                break;
        }

        fields->push_back(field);
    }
    return fields;
}

std::string
get_type_name(FieldPtr field)
{
    std::string name;
    switch (field->get_type()) {
        case (SchemaType::UINT64):
            name = "uint64";
            break;
        case (SchemaType::INT64):
            name = "int64";
            break;
        case (SchemaType::FLOAT64):
            name = "float64";
            break;

        case (SchemaType::TEXT):
            name = "text";
            break;

        case (SchemaType::BINARY):
            name = "binary";
            break;

        case (SchemaType::UINT32):
            name = "uint32";
            break;

        case (SchemaType::INT32):
            name = "int32";
            break;

        case (SchemaType::FLOAT32):
            name = "float32";
            break;

        case (SchemaType::UINT16):
            name = "uint16";
            break;

        case (SchemaType::INT16):
            name = "int16";
            break;

        case (SchemaType::UINT8):
            name = "uint8";
            break;

        case (SchemaType::INT8):
            name = "int8";
            break;

        case (SchemaType::BOOLEAN):
            name = "bool";
            break;

        case (SchemaType::NUMERIC):
            name = "numeric";
            break;
    }

    if (field->can_be_null()) {
        return name + "*";
    }

    return name;
}

void
process_extent(uint64_t offset,
               ExtentPtr extent)
{
    std::vector<std::string> field_names;

    std::cout << fmt::format("Offset: {}", offset) << std::endl;
    std::cout << fmt::format("Extent type={} xid={} row_size={} prev={}",
                             static_cast<uint8_t>(extent->header().type),
                             extent->header().xid, extent->header().row_size,
                             extent->header().prev_offset) << std::endl;

    auto fields = generate_fields(extent);
    for (auto field : *fields) {
        field_names.push_back(get_type_name(field));
    }

    std::cout << "Types: " << fmt::format("{}", fmt::join(field_names, ":")) << std::endl;

    std::cout << "Data:" << std::endl;
    for (auto row : *extent) {
        FieldTuple tuple(fields, &row);
        std::cout << tuple.to_string() << std::endl;
    }
    std::cout << std::endl << std::endl;
}

int
main(int argc,
     char *argv[])
{
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <filename>" << std::endl;
        return 1;
    }

    std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
    runners.emplace();
    runners->emplace_back(std::make_unique<IOMgrRunner>());

    // no logging
    springtail_init(runners, false, std::nullopt, LOG_NONE);

    // open the file
    auto handle = IOMgr::get_instance()->open(argv[1], IOMgr::READ, true);

    // read each extent from the file and process it
    uint64_t offset = 0;
    auto response = handle->read(offset);
    while (response->next_offset >= response->offset) {
        auto extent = std::make_shared<Extent>(response->data);

        process_extent(response->offset, extent);

        response = handle->read(response->next_offset);
    }

    return 0;
}
