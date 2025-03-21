#include <aws/core/Aws.h>
#include <aws/secretsmanager/SecretsManagerClient.h>
#include <aws/secretsmanager/model/GetSecretValueRequest.h>
#include <aws/secretsmanager/model/GetSecretValueResult.h>
#include <iostream>
#include <nlohmann/json.hpp> // For JSON parsing (optional)

#include <common/logging.hh>
#include <common/exception.hh>
#include <common/aws.hh>

namespace springtail {

    nlohmann::json
    AwsHelper::get_secret(const std::string& secret_name)
    {
        // Create Secrets Manager client with default credentials
        Aws::Client::ClientConfiguration config;
        Aws::SecretsManager::SecretsManagerClient client(config);

        Aws::SecretsManager::Model::GetSecretValueRequest request;
        request.SetSecretId(secret_name);

        auto outcome = client.GetSecretValue(request);
        if (!outcome.IsSuccess()) {
            SPDLOG_ERROR("Error fetching secret: {}", outcome.GetError().GetMessage());
            throw Error("Error fetching secret: " + outcome.GetError().GetMessage());
        }

        const auto& result = outcome.GetResult();
        std::string secret_value;
        if (result.GetSecretString().length() > 0) {
            secret_value = result.GetSecretString();
        } else {
            SPDLOG_ERROR("Secret is stored in binary format, which is unsupported");
            throw Error("Secret is stored in binary format, which is unsupported");
        }

        // Parse the secret value as JSON
        return nlohmann::json::parse(secret_value);
    }

} // namespace springtail
