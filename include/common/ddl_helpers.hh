#pragma once

#include <optional>
#include <string>
#include <ddl.pb.h>

namespace springtail {

// Extract the table ID from a DDL operation, if the operation references a specific table.
// Returns nullopt for namespace, user type, and partition operations that don't carry a tid.
inline std::optional<uint64_t> get_table_id(const proto::DDLOperation& op) {
    switch (op.operation_case()) {
        case proto::DDLOperation::kCreateTable:
            return op.create_table().tid();
        case proto::DDLOperation::kDropTable:
            return op.drop_table().tid();
        case proto::DDLOperation::kRenameTable:
            return op.rename_table().tid();
        case proto::DDLOperation::kSetNamespace:
            return op.set_namespace().tid();
        case proto::DDLOperation::kSetRlsEnabled:
            return op.set_rls_enabled().tid();
        case proto::DDLOperation::kSetRlsForced:
            return op.set_rls_forced().tid();
        case proto::DDLOperation::kColumnAdd:
            return op.column_add().tid();
        case proto::DDLOperation::kColumnDrop:
            return op.column_drop().tid();
        case proto::DDLOperation::kColumnRename:
            return op.column_rename().tid();
        case proto::DDLOperation::kColumnNullable:
            return op.column_nullable().tid();
        case proto::DDLOperation::kResync:
            return op.resync().tid();
        case proto::DDLOperation::kNoChange:
            return op.no_change().tid();
        case proto::DDLOperation::kResyncPartitions:
        case proto::DDLOperation::kNamespaceCreate:
        case proto::DDLOperation::kNamespaceAlter:
        case proto::DDLOperation::kNamespaceDrop:
        case proto::DDLOperation::kUsertypeCreate:
        case proto::DDLOperation::kUsertypeAlter:
        case proto::DDLOperation::kUsertypeDrop:
        case proto::DDLOperation::kAttachPartition:
        case proto::DDLOperation::kDetachPartition:
        case proto::DDLOperation::OPERATION_NOT_SET:
            return std::nullopt;
    }
    return std::nullopt;
}

// Return a human-readable action name for logging purposes.
inline std::string get_action_name(const proto::DDLOperation& op) {
    switch (op.operation_case()) {
        case proto::DDLOperation::kCreateTable:      return "create";
        case proto::DDLOperation::kDropTable:        return "drop";
        case proto::DDLOperation::kRenameTable:      return "rename";
        case proto::DDLOperation::kSetNamespace:     return "set_namespace";
        case proto::DDLOperation::kSetRlsEnabled:    return "set_rls_enabled";
        case proto::DDLOperation::kSetRlsForced:     return "set_rls_forced";
        case proto::DDLOperation::kColumnAdd:        return "col_add";
        case proto::DDLOperation::kColumnDrop:       return "col_drop";
        case proto::DDLOperation::kColumnRename:     return "col_rename";
        case proto::DDLOperation::kColumnNullable:   return "col_nullable";
        case proto::DDLOperation::kResync:           return "resync";
        case proto::DDLOperation::kNoChange:         return "no_change";
        case proto::DDLOperation::kResyncPartitions: return "resync_partitions";
        case proto::DDLOperation::kNamespaceCreate:  return "ns_create";
        case proto::DDLOperation::kNamespaceAlter:   return "ns_alter";
        case proto::DDLOperation::kNamespaceDrop:    return "ns_drop";
        case proto::DDLOperation::kUsertypeCreate:   return "ut_create";
        case proto::DDLOperation::kUsertypeAlter:    return "ut_alter";
        case proto::DDLOperation::kUsertypeDrop:     return "ut_drop";
        case proto::DDLOperation::kAttachPartition:  return "attach_partition";
        case proto::DDLOperation::kDetachPartition:  return "detach_partition";
        case proto::DDLOperation::OPERATION_NOT_SET: return "unknown";
    }
    return "unknown";
}

}  // namespace springtail
