#pragma once

#include <string>

#include <common/common.hh>
#include <common/properties.hh>

namespace springtail {
    namespace redis::db_state_change {
        /** Database state name that are used in reddis */
        static constexpr char const * const REDIS_STATE_STARTUP = "startup";
        static constexpr char const * const REDIS_STATE_INITIALIZE = "initialize";
        static constexpr char const * const REDIS_STATE_RUNNING = "running";
        static constexpr char const * const REDIS_STATE_SYNCING = "synchronizing";
        static constexpr char const * const REDIS_STATE_STOPPED = "stopped";

        /** Database state enums */
        enum DBState : uint8_t {
            DB_STATE_STARTUP = 0,
            DB_STATE_INITIALIZE = 1,
            DB_STATE_RUNNING = 2,
            DB_STATE_SYNCING = 3,
            DB_STATE_STOPPED = 4
        };

        /** Mapping from database state name to database state enum value */
        static std::map<std::string, DBState> db_state_map = {
            {REDIS_STATE_STARTUP, DB_STATE_STARTUP },
            {REDIS_STATE_INITIALIZE, DB_STATE_INITIALIZE },
            {REDIS_STATE_RUNNING, DB_STATE_RUNNING },
            {REDIS_STATE_SYNCING, DB_STATE_SYNCING },
            {REDIS_STATE_STOPPED, DB_STATE_STOPPED }
        };

        /** Mapping from database state enum value to database state name */
        static std::string db_state_to_name[] = {
            REDIS_STATE_STARTUP,
            REDIS_STATE_INITIALIZE,
            REDIS_STATE_RUNNING,
            REDIS_STATE_SYNCING,
            REDIS_STATE_STOPPED
        };

        /**
         * @brief Functio for parsing database state change notification from redis
         *
         * @param msg - message from redis
         * @param db_id - database id recorded in redis
         * @param state - state of the database in redis
         */
        static inline void parse_db_state_change(const std::string &msg, uint64_t &db_id, DBState &state) {
            std::vector<std::string> msg_parts;
            common::split_string(":", msg, msg_parts);
            assert(msg_parts.size() == 2);
            db_id = stoull(msg_parts[0]);
            state = db_state_map[msg_parts[1]];
        }

        /** Function for getting database state enum from properties *//**
         * @brief Function for getting database state enum from properties
         *
         * @param db_id - database id
         * @return DBState
         */
        static inline DBState get_db_state(uint64_t db_id) {
            return db_state_map[Properties::get_db_state(db_id)];
        }
    }
}