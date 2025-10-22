#include <aws/core/Aws.h>
#include <aws/secretsmanager/SecretsManagerClient.h>
#include <aws/secretsmanager/model/GetSecretValueRequest.h>
#include <aws/secretsmanager/model/GetSecretValueResult.h>
#include <nlohmann/json.hpp>

#include <common/logging.hh>
#include <common/exception.hh>
#include <common/aws.hh>
#include <common/properties.hh>
#include <common/json.hh>

namespace springtail {

    void
    AwsHelper::_create_secrets_manager_client()
    {
        // get aws_mock_override from properties org_config
        _aws_secrets_overrides = Properties::get(Properties::AWS_USERS_OVERRIDE);
        if (!_aws_secrets_overrides.is_null()) {
            return;
        }

        Aws::Client::ClientConfiguration config;
        _client = std::make_shared<Aws::SecretsManager::SecretsManagerClient>(config);
    }

    nlohmann::json
    AwsHelper::get_secret(const std::string& secret_name)
    {
        // Create Secrets Manager client with default credentials
        if (!_client) {
            _create_secrets_manager_client();
        }

        if (!_aws_secrets_overrides.is_null()) {
            return _aws_secrets_overrides;
        }

        Aws::SecretsManager::Model::GetSecretValueRequest request;
        request.SetSecretId(secret_name);

        auto outcome = _client->GetSecretValue(request);
        if (!outcome.IsSuccess()) {
            LOG_ERROR("Error fetching secret: {}, error: {}", secret_name, outcome.GetError().GetMessage());
            throw Error("Error fetching secret: " + outcome.GetError().GetMessage());
        }

        const auto& result = outcome.GetResult();
        std::string secret_value;
        if (result.GetSecretString().length() > 0) {
            secret_value = result.GetSecretString();
        } else {
            LOG_ERROR("Secret is stored in binary format, which is unsupported");
            throw Error("Secret is stored in binary format, which is unsupported");
        }

        // Parse the secret value as JSON
        return nlohmann::json::parse(secret_value);
    }

} // namespace springtail
