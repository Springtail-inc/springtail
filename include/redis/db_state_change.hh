#pragma once

#include <string>

#include <common/common.hh>
#include <common/properties.hh>

namespace springtail::redis::db_state_change {

/** Database state names that are used in redis */
static constexpr char const * const REDIS_STATE_INITIALIZE = "initialize";
static constexpr char const * const REDIS_STATE_RUNNING = "running";
static constexpr char const * const REDIS_STATE_STOPPED = "stopped";
static constexpr char const * const REDIS_STATE_FAILED = "failed";
static constexpr char const * const REDIS_STATE_UNKNOWN = "unknown";

/** Database state enums */
enum DBState : uint8_t {
    DB_STATE_INITIALIZE = 0,
    DB_STATE_RUNNING = 1,
    DB_STATE_STOPPED = 2,
    DB_STATE_FAILED = 3,
    DB_STATE_UNKNOWN = 4
};

/** Mapping from database state name to database state enum value */
static std::map<std::string, DBState> db_state_map = {
    {REDIS_STATE_INITIALIZE, DB_STATE_INITIALIZE },
    {REDIS_STATE_RUNNING, DB_STATE_RUNNING },
    {REDIS_STATE_STOPPED, DB_STATE_STOPPED },
    {REDIS_STATE_FAILED, DB_STATE_FAILED},
    {REDIS_STATE_UNKNOWN, DB_STATE_UNKNOWN}
};

/** Mapping from database state enum value to database state name */
static std::string db_state_to_name[] = {
    REDIS_STATE_INITIALIZE,
    REDIS_STATE_RUNNING,
    REDIS_STATE_STOPPED,
    REDIS_STATE_FAILED,
    REDIS_STATE_UNKNOWN
};

/**
 * @brief Function for getting database state enum from properties
 *
 * @param db_id - database id
 * @return DBState
 */
static inline DBState get_db_state(uint64_t db_id) {
    return db_state_map[Properties::get_db_state(db_id)];
}

/**
 * @brief Set the db state by converting enum to string and passing it to Properties
 *
 * @param db_id - database id
 * @param state - new database state
 */
static inline void set_db_state(uint64_t db_id, DBState state) {
    Properties::set_db_state(db_id, db_state_to_name[state]);
}

}
