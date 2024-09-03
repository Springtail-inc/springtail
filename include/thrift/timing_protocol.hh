#pragma once

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TProtocolDecorator.h>

#include <common/logging.hh>

namespace transport = apache::thrift::transport;
namespace protocol = apache::thrift::protocol;

namespace springtail::thrift {

    /**
     * @brief Timing protocol decorator
     * Use to wrap a TProtocol object and log the RPC calls
     */
    class TimingProtocol : public protocol::TProtocolDecorator {
    public:
        explicit TimingProtocol(std::shared_ptr<protocol::TProtocol> protocol)
            : protocol::TProtocolDecorator(protocol) {}

        /**
         * @brief Override read message begin
         * when server receives msg
         */
        uint32_t readMessageBegin_virt(std::string& name,
                                       protocol::TMessageType& messageType,
                                       int32_t& seqid) override
        {
            uint32_t result = TProtocolDecorator::readMessageBegin_virt(name, messageType, seqid);
            SPDLOG_INFO("RPC call, read message: {}", name);
            return result;
        }

        /**
         * @brief Override write message begin
         * when server sends msg reply
         */
        uint32_t writeMessageBegin_virt(const std::string& name,
                                        const protocol::TMessageType messageType,
                                        const int32_t seqid) override
        {
            uint32_t result = TProtocolDecorator::writeMessageBegin_virt(name, messageType, seqid);
            SPDLOG_INFO("RPC call, write message: {}", name);
            return result;
        }
    };

    /**
     * @brief Factor for creating TimingProtocol objects
     * Use to wrap a TCompactProtocol object and log the RPC calls
     * Use when creating a server
     */
    class TimingProtocolFactory : public protocol::TCompactProtocolFactory {
    public:
        TimingProtocolFactory() = default;

        // Override the getProtocol method
        std::shared_ptr<protocol::TProtocol>
        getProtocol(std::shared_ptr<transport::TTransport> trans) override {
            // Create a TCompactProtocol instance using the base factory method
            std::shared_ptr<protocol::TProtocol> compactProtocol =
                protocol::TCompactProtocolFactory::getProtocol(trans);

            // Wrap it with TimingProtocol
            return std::make_shared<TimingProtocol>(compactProtocol);
        }
    };
}