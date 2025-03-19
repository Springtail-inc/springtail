#pragma once

#include <aws/core/Aws.h>
#include <nlohmann/json.hpp>

namespace springtail {
    /**
     * @brief AWS class with static helpers for AWS operations
     */
    class AwsHelper {
    public:
        static inline constexpr char DB_USERS_SECRET[] = "sk/{}/{}/aws/dbi/{}/primary_db_password";

        AwsHelper() { Aws::InitAPI(_options); }
        ~AwsHelper() { Aws::ShutdownAPI(_options); }

        /**
         * @brief Get the secret object
         * @param secret_name name of the secret
         * @return nlohmann::json secret parsed as json
         */
        nlohmann::json get_secret(const std::string &secret_name);

    private:
        Aws::SDKOptions _options;
    };
    using AwsHelperPtr = std::shared_ptr<AwsHelper>;

} // namespace springtail
