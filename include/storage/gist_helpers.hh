#pragma once

#include <vector>
#include <string>

#include <storage/field.hh>

#include <common/constants.hh>
#include <storage/extent.hh>
#include <storage/schema.hh>

namespace springtail::gist_helpers {

    /**
     * @brief Extract a GIST entry from a tuple
     */
    GistEntry extract_gist_entry_from_tuple(TuplePtr tuple, ExtentSchemaPtr schema, const std::vector<std::string>& opclass_names);

    /**
     * @brief Read a branch entry from a row
     */
    GistEntry read_branch_entry_from_row(const Extent::Row& row, ExtentSchemaPtr schema, const std::vector<std::string>& opclass_names);

    /**
     * @brief Read a leaf entry from a row
     */
    GistEntry read_leaf_entry_from_row(const Extent::Row& row, ExtentSchemaPtr schema, const std::vector<std::string>& opclass_names);

    /**
     * @brief Write a leaf entry to a row
     */
    void write_leaf_row_from_entry(Extent::Row& row, const GistEntry& entry, ExtentSchemaPtr schema, const std::vector<std::string>& opclass_names);

    /**
     * @brief Write a branch entry to a row
     */
    void write_branch_row_from_unions(Extent::Row& row, const GistEntry& key, uint64_t child_page_id, ExtentSchemaPtr schema, const std::vector<std::string>& opclass_names);

    /**
     * @brief Extract child page ID from a branch row
     */
    uint64_t extract_child_pageid_from_row(const Extent::Row& row, ExtentSchemaPtr schema);

    /**
     * @brief Compute the GIST penalty for a new entry
     *
     * @param existing_entry
     * @param new_entry
     * @param opclass_names
     * @return double
     */
    double compute_gist_penalty(const GistEntry& existing_entry, const GistEntry& new_entry, const std::vector<std::string>& opclass_names);

    /**
     * @brief Compute the union of a set of entries
     *
     * @param entries
     * @param union_entry
     * @param opclass_names
     */
    void compute_union(const std::vector<GistEntry>& entries, GistEntry& union_entry, const std::vector<std::string>& opclass_names);

    /**
     * @brief Compute the pick split for a set of entries
     *
     * @param entries
     * @param out_left
     * @param out_right
     * @param opclass_names
     */
    void compute_picksplit(const std::vector<GistEntry>& entries, std::vector<GistEntry>& out_left, std::vector<GistEntry>& out_right, const std::vector<std::string>& opclass_names);
}
