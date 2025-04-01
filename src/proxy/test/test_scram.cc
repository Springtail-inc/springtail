#include <gtest/gtest.h>
#include <vector>

#include <common/init.hh>
#include <common/logging.hh>

#include <proxy/user_mgr.hh>
#include <proxy/exception.hh>
#include <proxy/auth/scram.hh>
#include <proxy/auth/scram-common.h>

using namespace springtail;
using namespace springtail::pg_proxy;

class ScramAuth_Test : public testing::Test {
protected:
    void validate_scram(UserPtr ss_user, UserPtr cs_user)
    {
        // Check if the user has a valid password type
        ASSERT_TRUE(ss_user->password_type() == pg_proxy::PasswordType::SCRAM ||
                    ss_user->password_type() == pg_proxy::PasswordType::TEXT);
        ASSERT_TRUE(cs_user->password_type() == pg_proxy::PasswordType::SCRAM ||
                    cs_user->password_type() == pg_proxy::PasswordType::TEXT);

        UserLoginPtr ss_login = ss_user->get_user_login();
        UserLoginPtr cs_login = cs_user->get_user_login();
        ASSERT_NE(ss_login, nullptr);
        ASSERT_NE(cs_login, nullptr);

        std::string client_first_msg = ss_handle_scram_auth(ss_user, ss_login);

        std::string server_first_msg = cs_handle_scram_auth(cs_user, cs_login, client_first_msg);

        std::string client_final_msg = ss_handle_auth_scram_continue(ss_user, ss_login, server_first_msg);

        std::string server_final_msg = cs_handle_scram_auth_continue(cs_user, cs_login, client_final_msg);

        ss_handle_auth_scram_complete(ss_user, ss_login, server_final_msg);
    }

    std::string
    ss_handle_scram_auth(UserPtr user, UserLoginPtr ss_login)
    {
        // ServerSession _handle_scram_auth()
        // sets client_nonce and client_first_message_bare in scram_state
        char *client_first_message = build_client_first_message(&ss_login->scram_state);
        if (client_first_message == nullptr) {
            SPDLOG_ERROR("Failed to build client first message");
            throw ProxyAuthError();
        }

        std::string result = std::string(client_first_message);
        free(client_first_message);
        return result;
    }

    std::string
    cs_handle_scram_auth(UserPtr user, UserLoginPtr cs_login, const std::string_view data)
    {
        std::string raw(data);

        // ClientSession _handle_scram_auth_()
        if (!read_client_first_message(raw.data(), &cs_login->scram_state.cbind_flag,
                                       &cs_login->scram_state.client_first_message_bare,
                                       &cs_login->scram_state.client_nonce)) {
            SPDLOG_ERROR("Failed to read client first message");
            throw ProxyAuthError();
        }

        ::PasswordType type;  // see scram.hh
        switch (cs_login->type) {
            case SCRAM:
                type = PASSWORD_TYPE_SCRAM_SHA_256;
                break;
            case TEXT:
                type = PASSWORD_TYPE_PLAINTEXT;
                break;
            default:
                SPDLOG_ERROR("Invalid password type for SCRAM authentication");
                throw ProxyAuthError();
        }

        if (!build_server_first_message(&cs_login->scram_state, user->username().c_str(),
                                        cs_login->password.c_str(), type)) {
            SPDLOG_ERROR("Failed to build server first message");
            throw ProxyAuthError();
        }

        std::string result = std::string(cs_login->scram_state.server_first_message);
        return result;
    }

    std::string
    ss_handle_auth_scram_continue(UserPtr user, UserLoginPtr ss_login, const std::string_view data)
    {
        // ServerSession _handle_auth_scram_continue()
        if (ss_login->scram_state.client_nonce == nullptr) {
            SPDLOG_ERROR("No client nonce set");
            throw ProxyAuthError();
        }

        if (ss_login->scram_state.server_first_message != nullptr) {
            SPDLOG_ERROR("Received second SCRAM-SHA-256 continue message");
            throw ProxyAuthError();
        }

        int salt_len;
        std::string input(data);

        // sets: server_first_message, server_nonce, salt, iterations in scram_state
        if (!read_server_first_message(&ss_login->scram_state, input.data(),
                                       &ss_login->scram_state.server_nonce, &ss_login->scram_state.salt,
                                       &salt_len, &ss_login->scram_state.iterations)) {
            SPDLOG_ERROR("Failed to read server first message");
            throw ProxyAuthError();
        }

        PgUser pg_user;
        // get right key/password for the user for scram
        if (ss_login->type == SCRAM) {
            pg_user.scram_ClientKey = ss_login->scram_state.ClientKey;
            pg_user.has_scram_keys = true;
        } else if (ss_login->type == TEXT) {
            pg_user.has_scram_keys = false;
            pg_user.passwd = ss_login->password.c_str();
            SPDLOG_DEBUG("Using TEXT password for SCRAM for password: {}", pg_user.passwd);
        } else {
            SPDLOG_ERROR("Invalid password type for SCRAM");
            throw ProxyAuthError();
        }

        char *client_final_message = build_client_final_message(
            &ss_login->scram_state, &pg_user, ss_login->scram_state.server_nonce, ss_login->scram_state.salt,
            salt_len, ss_login->scram_state.iterations);

        if (client_final_message == nullptr) {
            SPDLOG_ERROR("Failed to build client final message");
            throw ProxyAuthError();
        }

        std::string result = std::string(client_final_message);
        free(client_final_message);
        return result;
    }

    std::string
    cs_handle_scram_auth_continue(UserPtr user, UserLoginPtr cs_login, const std::string_view data)
    {
        // ClientSession _handle_scram_auth_continue()
        std::string raw(data);
        const char *client_final_nonce = nullptr;
        char *proof = nullptr;

        // decode the final message from client
        if (!read_client_final_message(&cs_login->scram_state,
                                       reinterpret_cast<const uint8_t *>(data.data()), raw.data(),
                                       &client_final_nonce, &proof)) {
            SPDLOG_ERROR("Failed to read client final message");
            throw ProxyAuthError();
        }

        // verify the nonce and the proof from client
        // verify_client_proof sets client key in scram state
        if (!verify_final_nonce(&cs_login->scram_state, client_final_nonce) ||
            !verify_client_proof(&cs_login->scram_state, proof)) {
            SPDLOG_ERROR("Invalid SCRAM response (nonce or proof does not match)");
            free(proof);
            throw ProxyAuthError();
        }
        free(proof);

        // after verifying the client proof, we now have the client key
        user->set_client_scram_key(cs_login->scram_state.ClientKey);

        // finally send the final message to the client
        char *server_final_message = build_server_final_message(&cs_login->scram_state);
        if (server_final_message == nullptr) {
            SPDLOG_ERROR("Failed to build server final message");
            throw ProxyAuthError();
        }
        std::string result = std::string(server_final_message);
        free(server_final_message);

        return result;
    }

    void
    ss_handle_auth_scram_complete(UserPtr user, UserLoginPtr ss_login, const std::string_view data)
    {
        // ServerSession _handle_auth_scram_complete()
        // make sure we are in right flow
        if (ss_login->scram_state.server_first_message == nullptr) {
            SPDLOG_ERROR("No server first message set");
            throw ProxyAuthError();
        }

        std::string input(data);
        char ServerSignature[SHA256_DIGEST_LENGTH];

        // decode the final message from server
        if (!read_server_final_message(input.data(), ServerSignature)) {
            SPDLOG_ERROR("Failed to read server final message");
            throw ProxyAuthError();
        }

        PgUser pg_user;
        if (ss_login->type == SCRAM) {
            pg_user.scram_ClientKey = ss_login->scram_state.ClientKey;
            pg_user.scram_ServerKey = ss_login->scram_state.ServerKey;
            pg_user.has_scram_keys = true;
        } else if (ss_login->type == TEXT) {
            pg_user.has_scram_keys = false;
        }

        // last step, verify the server signature
        if (!verify_server_signature(&ss_login->scram_state, &pg_user, ServerSignature)) {
            SPDLOG_ERROR("Failed to verify server signature");
            throw ProxyAuthError();
        }

        return;
    }
};

TEST_F(ScramAuth_Test, ScramAuthMatch)
{
    // Test SCRAM authentication
    UserPtr cs_user = std::make_shared<User>("claire", "code-veronica", pg_proxy::PasswordType::TEXT);
    UserPtr ss_user = std::make_shared<User>("claire", "code-veronica", pg_proxy::PasswordType::TEXT);
    ASSERT_NO_THROW(validate_scram(ss_user, cs_user));

    cs_user = std::make_shared<User>("john", "foobar", pg_proxy::PasswordType::TEXT);
    ss_user = std::make_shared<User>("john", "foobar", pg_proxy::PasswordType::TEXT);
    ASSERT_NO_THROW(validate_scram(ss_user, cs_user));
}

TEST_F(ScramAuth_Test, ScramTypeSCRAM)
{
    UserPtr cs_user = std::make_shared<User>("claire",
        "SCRAM-SHA-256$4096:wQ+icMtfPZSJfyu4kLLwgA==$CyJAjykoQ+nxpO0oH1/XOUwXpeZfNNbaDy+KLWJ6nNc=:7TmdcGjef3mM1cE0naHWetWOfROhnwY0c7Rb8tpbGeY=",
        pg_proxy::PasswordType::SCRAM);
    UserPtr ss_user = std::make_shared<User>("claire", "code-veronica", pg_proxy::PasswordType::TEXT);
    ASSERT_NO_THROW(validate_scram(ss_user, cs_user));
}

TEST_F(ScramAuth_Test, ScramAuthNoMatch)
{
    UserPtr cs_user = std::make_shared<User>("john", "foobar", pg_proxy::PasswordType::TEXT);
    UserPtr ss_user = std::make_shared<User>("john", "foobar2", pg_proxy::PasswordType::TEXT);
    ASSERT_THROW(validate_scram(ss_user, cs_user), ProxyAuthError);
}