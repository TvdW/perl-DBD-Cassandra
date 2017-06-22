void cc_type_destroy(pTHX_ struct cc_type *type);
int unpack_type_nocroak(pTHX_ char *input, STRLEN len, STRLEN *pos, struct cc_type *output);
void unpack_type(pTHX_ char *input, STRLEN len, STRLEN *pos, struct cc_type *output);



int unpack_type_nocroak(pTHX_ char *input, STRLEN len, STRLEN *pos, struct cc_type *output)
{
    if (UNLIKELY(unpack_short_nocroak(aTHX_ input, len, pos, &output->type_id) != 0))
        return -1;

    if (output->type_id > 0 && output->type_id < 0x20) {
        // Primitives. No further decoding needed

    } else if (output->type_id == CC_TYPE_CUSTOM) {
        char *custom_type;
        STRLEN type_length;
        unpack_string(aTHX_ input, len, pos, &custom_type, &type_length);
        const char *marshal_prefix = "org.apache.cassandra.db.marshal.";
        if (type_length > strlen(marshal_prefix) && !memcmp(marshal_prefix, custom_type, strlen(marshal_prefix))) {
            char *marshal_type;
            STRLEN marshal_type_length;

            marshal_type = custom_type + strlen(marshal_prefix);
            marshal_type_length = type_length - strlen(marshal_prefix);
            switch(marshal_type_length) {
                case 8:
                    if (!memcmp(marshal_type, "UTF8Type", 8)) { output->type_id = CC_TYPE_VARCHAR; break; }
                    if (!memcmp(marshal_type, "UUIDType", 8)) { output->type_id = CC_TYPE_UUID; break; }
                    if (!memcmp(marshal_type, "TimeType", 8)) { output->type_id = CC_TYPE_TIME; break; }
                    if (!memcmp(marshal_type, "ByteType", 8)) { output->type_id = CC_TYPE_TINYINT; break; }
                    if (!memcmp(marshal_type, "DateType", 8)) { output->type_id = CC_TYPE_DATE; break; }
                    if (!memcmp(marshal_type, "LongType", 8)) { output->type_id = CC_TYPE_BIGINT; break; }
                    break;
                case 9:
                    if (!memcmp(marshal_type, "AsciiType", 9)) { output->type_id = CC_TYPE_ASCII; break; }
                    if (!memcmp(marshal_type, "Int32Type", 9)) { output->type_id = CC_TYPE_INT; break; }
                    if (!memcmp(marshal_type, "BytesType", 9)) { output->type_id = CC_TYPE_BLOB; break; }
                    if (!memcmp(marshal_type, "FloatType", 9)) { output->type_id = CC_TYPE_FLOAT; break; }
                    if (!memcmp(marshal_type, "ShortType", 9)) { output->type_id = CC_TYPE_SMALLINT; break; }
                    break;
                case 10:
                    if (!memcmp(marshal_type, "DoubleType", 10)) { output->type_id = CC_TYPE_DOUBLE; break; }
                    break;
                case 11:
                    if (!memcmp(marshal_type, "BooleanType", 11)) { output->type_id = CC_TYPE_BOOLEAN; break; }
                    if (!memcmp(marshal_type, "DecimalType", 11)) { output->type_id = CC_TYPE_DECIMAL; break; }
                    if (!memcmp(marshal_type, "IntegerType", 11)) { output->type_id = CC_TYPE_VARINT; break; }
                    break;
                case 12:
                    if (!memcmp(marshal_type, "TimeUUIDType", 12)) { output->type_id = CC_TYPE_TIMEUUID; break; }
                    break;
                case 13:
                    if (!memcmp(marshal_type, "TimestampType", 13)) { output->type_id = CC_TYPE_TIMESTAMP; break; }
                    break;
                case 14:
                    if (!memcmp(marshal_type, "SimpleDateType", 14)) { output->type_id = CC_TYPE_DATE; break; }
                    break;
                case 15:
                    if (!memcmp(marshal_type, "InetAddressType", 15)) { output->type_id = CC_TYPE_INET; break; }
                    break;
                case 17:
                    if (!memcmp(marshal_type, "CounterColumnType", 17)) { output->type_id = CC_TYPE_COUNTER; break; }
                    break;
            }
        }

        // If we made it this far, it's not a type we understand. So just copy the name and we're done.
        Newxz(output->custom_name, type_length+1, char);
        memcpy(output->custom_name, custom_type, type_length);

    } else if (output->type_id == CC_TYPE_LIST) {
        struct cc_type *inner;
        Newxz(inner, 1, struct cc_type);
        output->inner_type = inner;

        if (unpack_type_nocroak(aTHX_ input, len, pos, inner) != 0) {
            return -3;
        }

    } else if (output->type_id == CC_TYPE_MAP) {
        Newxz(output->inner_type, 2, struct cc_type);

        if (unpack_type_nocroak(aTHX_ input, len, pos, &output->inner_type[0]) != 0) {
            return -3;
        }
        if (unpack_type_nocroak(aTHX_ input, len, pos, &output->inner_type[1]) != 0) {
            return -3;
        }

    } else if (output->type_id == CC_TYPE_SET) {
        struct cc_type *inner;
        Newxz(inner, 1, struct cc_type);
        output->inner_type = inner;

        if (unpack_type_nocroak(aTHX_ input, len, pos, inner) != 0) {
            return -3;
        }

    } else if (output->type_id == CC_TYPE_UDT) {
        // UDT
        return -3;

    } else if (output->type_id == CC_TYPE_TUPLE) {
        // Tuple
        return -3;

    } else {
        return -2;
    }

    return 0;
}

void unpack_type(pTHX_ char *input, STRLEN len, STRLEN *pos, struct cc_type *output)
{
    if (UNLIKELY(unpack_type_nocroak(aTHX_ input, len, pos, output) != 0)) {
        cc_type_destroy(aTHX_ output);
        croak("unpack_type: invalid input. Data corrupted?");
    }
}

void cc_type_destroy(pTHX_ struct cc_type *type)
{
    if (type->type_id == CC_TYPE_LIST || type->type_id == CC_TYPE_SET) {
        if (type->inner_type != NULL) {
            cc_type_destroy(aTHX_ type->inner_type);
            Safefree(type->inner_type);
            type->inner_type = NULL;
        }

    } else if (type->type_id == CC_TYPE_MAP) {
        if (type->inner_type != NULL) {
            cc_type_destroy(aTHX_ &type->inner_type[0]);
            cc_type_destroy(aTHX_ &type->inner_type[1]);
            Safefree(type->inner_type);
            type->inner_type = NULL;
        }

    } else if (type->type_id == CC_TYPE_CUSTOM) {
        if (type->custom_name != NULL) {
            Safefree(type->custom_name);
            type->custom_name = NULL;
        }
    }
}