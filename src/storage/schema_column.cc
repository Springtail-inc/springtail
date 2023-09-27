#include <tao/json.hpp>

#include <schema/schema_column.hh>
#include <storage/field.hh>

namespace st_storage {

    //// SchemaColumn

    SchemaColumn::SchemaColumn(const SchemaColumnDetails &details)
        : _type(details.type()),
          _nullable(details.nullable()),
          _default_null(details.default_null())
    { }

    void
    SchemaColumn::_position_helper(uint32_t &cur_byte,
                                   uint16_t &null_bit,
                                   uint8_t size)
    {
        // if the field is nullable, reserve a null bit for it
        if (_nullable) {
            _null_pos = null_bit++;
        }

        // set the position in the row and move forward by the field size
        _row_pos = cur_byte;
        cur_byte += size;
    }


    //// SchemaColumnBoolean

    void SchemaColumnBoolean::update_position(uint32_t &cur_byte,
                                              uint16_t &bool_bit,
                                              uint16_t &null_bit,
                                              bool strict)
    {
        // update the position details based on the size of the field
        _position_helper(cur_byte, null_bit, 0);

        // then specialized code for boolean bit management
        _bool_pos = bool_bit++;
    }

    std::shared_ptr<Field>
    SchemaColumnBoolean::get_field(uint32_t bool_offset, uint32_t null_offset)
    {
        uint8_t bool_bit = _bool_pos & 0x07;
        uint32_t row_pos = bool_offset + (_bool_pos >> 7);

        if (_nullable) {
            uint8_t null_bit = _null_pos & 0x07;
            uint32_t null_pos = null_offset + (_null_pos >> 7);

            return std::make_shared<NullableBoolField>(row_pos, bool_bit, null_pos, null_bit);
        } else {
            return std::make_shared<BoolField>(row_pos, bool_bit);
        }
    }


    //// SchemaColumnNumber
    template <class T>
    void
    SchemaColumnNumber<T>::update_position(uint32_t &cur_byte,
                                           uint16_t &bool_bit,
                                           uint16_t &null_bit,
                                           bool strict)
    {
        // update the position details based on the size of the field
        _position_helper(cur_byte, null_bit, sizeof(T));
    }

    // declare the possible template instantiations
    template void SchemaColumnNumber<uint64_t>::update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);
    template void SchemaColumnNumber<uint32_t>::update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);
    template void SchemaColumnNumber<uint16_t>::update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);
    template void SchemaColumnNumber<uint8_t>::update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);
    template void SchemaColumnNumber<int64_t>::update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);
    template void SchemaColumnNumber<int32_t>::update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);
    template void SchemaColumnNumber<int16_t>::update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);
    template void SchemaColumnNumber<int8_t>::update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);
    template void SchemaColumnNumber<float>::update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);
    template void SchemaColumnNumber<double>::update_position(uint32_t &cur_byte, uint16_t &bool_bit, uint16_t &null_bit, bool strict);


    template <class T>
    std::shared_ptr<Field>
    SchemaColumnNumber<T>::get_field(uint32_t bool_offset, uint32_t null_offset)
    {
        if (_nullable) {
            uint8_t null_bit = _null_pos & 0x07;
            uint32_t null_pos = null_offset + (_null_pos >> 7);

            return std::make_shared<NullableNumberField<T>>(_row_pos, null_pos, null_bit);
        } else {
            return std::make_shared<NumberField<T>>(_row_pos);
        }
    }

    // declare the possible template instantiations
    template std::shared_ptr<Field> SchemaColumnNumber<uint64_t>::get_field(uint32_t bool_offset, uint32_t null_offset);
    template std::shared_ptr<Field> SchemaColumnNumber<uint32_t>::get_field(uint32_t bool_offset, uint32_t null_offset);
    template std::shared_ptr<Field> SchemaColumnNumber<uint16_t>::get_field(uint32_t bool_offset, uint32_t null_offset);
    template std::shared_ptr<Field> SchemaColumnNumber<uint8_t>::get_field(uint32_t bool_offset, uint32_t null_offset);
    template std::shared_ptr<Field> SchemaColumnNumber<int64_t>::get_field(uint32_t bool_offset, uint32_t null_offset);
    template std::shared_ptr<Field> SchemaColumnNumber<int32_t>::get_field(uint32_t bool_offset, uint32_t null_offset);
    template std::shared_ptr<Field> SchemaColumnNumber<int16_t>::get_field(uint32_t bool_offset, uint32_t null_offset);
    template std::shared_ptr<Field> SchemaColumnNumber<int8_t>::get_field(uint32_t bool_offset, uint32_t null_offset);
    template std::shared_ptr<Field> SchemaColumnNumber<float>::get_field(uint32_t bool_offset, uint32_t null_offset);
    template std::shared_ptr<Field> SchemaColumnNumber<double>::get_field(uint32_t bool_offset, uint32_t null_offset);


    //// SchemaColumnText
    void
    SchemaColumnText::update_position(uint32_t &cur_byte,
                                      uint16_t &bool_bit,
                                      uint16_t &null_bit,
                                      bool strict)
    {
        // update the position details based on the size of the field
        _position_helper(cur_byte, null_bit, 4);
    }

    std::shared_ptr<Field>
    SchemaColumnText::get_field(uint32_t bool_offset, uint32_t null_offset)
    {
        if (_nullable) {
            uint8_t null_bit = _null_pos & 0x07;
            uint32_t null_pos = null_offset + (_null_pos >> 7);

            return std::make_shared<NullableTextField>(_row_pos, null_pos, null_bit);
        } else {
            return std::make_shared<TextField>(_row_pos);
        }
    }


    //// SchemaColumnBinary
    void
    SchemaColumnBinary::update_position(uint32_t &cur_byte,
                                        uint16_t &bool_bit,
                                        uint16_t &null_bit,
                                        bool strict)
    {
        // update the position details based on the size of the field
        _position_helper(cur_byte, null_bit, 8);
    }

    std::shared_ptr<Field>
    SchemaColumnBinary::get_field(uint32_t bool_offset, uint32_t null_offset)
    {
        if (_nullable) {
            uint8_t null_bit = _null_pos & 0x07;
            uint32_t null_pos = null_offset + (_null_pos >> 7);

            return std::make_shared<NullableBinaryField>(_row_pos, null_pos, null_bit);
        } else {
            return std::make_shared<BinaryField>(_row_pos);
        }
    }

}
