#include <aws/core/Aws.h>
#include <aws/secretsmanager/SecretsManagerClient.h>
#include <aws/secretsmanager/model/GetSecretValueRequest.h>
#include <aws/secretsmanager/model/GetSecretValueResult.h>
#include <iostream>
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
        Aws::Client::ClientConfiguration client_config;
        // get aws_mock_override from properties org_config
        nlohmann::json org_config = Properties::get(Properties::ORG_CONFIG);
        bool aws_mock_override = Json::get_or<bool>(org_config, "aws_mock_override", false);

        Aws::Client::ClientConfiguration config;
        // if using a mocking service like localstack, override the endpoint
        if (aws_mock_override) {
            SPDLOG_INFO("Using AWS mock override");
            config.endpointOverride = "http://localhost:4566";
            config.region = "us-east-1"; // doesn't matter
            config.scheme = Aws::Http::Scheme::HTTP;
        }

        _client = std::make_shared<Aws::SecretsManager::SecretsManagerClient>(config);
    }

    nlohmann::json
    AwsHelper::get_secret(const std::string& secret_name)
    {
        // Create Secrets Manager client with default credentials
        if (!_client) {
            _create_secrets_manager_client();
        }

        Aws::SecretsManager::Model::GetSecretValueRequest request;
        request.SetSecretId(secret_name);

        auto outcome = _client->GetSecretValue(request);
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
