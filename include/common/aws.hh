#pragma once

#include <aws/core/Aws.h>
#include <aws/secretsmanager/SecretsManagerClient.h>
#include <nlohmann/json.hpp>

namespace springtail {
    /**
     * @brief AWS class with static helpers for AWS operations
     */
    class AwsHelper {
    public:
        static inline constexpr char DB_USERS_SECRET[] = "sk/{}/{}/aws/dbi/{}/primary_db_password";

        AwsHelper() { Aws::InitAPI(_options); }

        ~AwsHelper() {
            Aws::ShutdownAPI(_options);
            if (_client) {
                _client = nullptr;
            }
        }

        /**
         * @brief Get the secret object
         * @param secret_name name of the secret
         * @return nlohmann::json secret parsed as json
         */
        nlohmann::json get_secret(const std::string &secret_name);

    private:
        Aws::SDKOptions _options;
        std::shared_ptr<Aws::SecretsManager::SecretsManagerClient> _client = nullptr;

        void _create_secrets_manager_client();
    };
    using AwsHelperPtr = std::shared_ptr<AwsHelper>;

} // namespace springtail
