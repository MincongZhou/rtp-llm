#include "gtest/gtest.h"
#include "rtp_llm/cpp/model_rpc/RemoteRpcServiceImpl.h"

namespace rtp_llm {

TEST(RemoteRpcServiceImplTest, RejectsLegacyPDConfigurationBeforeEngineInitialization) {
    RemoteRpcServiceImpl service;
    EngineInitParams     params;
    params.pd_sep_config.decode_entrance = false;
    for (auto role : {RoleType::PREFILL, RoleType::DECODE}) {
        params.pd_sep_config.role_type = role;
        const auto status              = service.init(params, nullptr, py::object{});
        EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
        EXPECT_NE(status.error_message().find("decode_entrance=true"), std::string::npos);
    }
}

TEST(RemoteRpcServiceImplTest, RetiredPDMethodsReturnUnimplemented) {
    RemoteRpcServiceImpl service;
    grpc::ServerContext  context;
    EXPECT_EQ(service.RemoteGenerate(&context, nullptr).error_code(), grpc::StatusCode::UNIMPLEMENTED);
    EXPECT_EQ(service.RemoteLoad(&context, nullptr, nullptr).error_code(), grpc::StatusCode::UNIMPLEMENTED);
    EXPECT_EQ(service.RemoteFinish(&context, nullptr, nullptr).error_code(), grpc::StatusCode::UNIMPLEMENTED);
    EXPECT_EQ(service.EnqueueGroup(&context, nullptr, nullptr).error_code(), grpc::StatusCode::UNIMPLEMENTED);
    EXPECT_EQ(service.FetchResponse(&context, nullptr, nullptr).error_code(), grpc::StatusCode::UNIMPLEMENTED);
    EXPECT_EQ(service.Cancel(&context, nullptr, nullptr).error_code(), grpc::StatusCode::UNIMPLEMENTED);
}

TEST(RemoteRpcServiceImplTest, RejectsUnsupportedPDRoleBeforeEngineInitialization) {
    RemoteRpcServiceImpl service;
    EngineInitParams     params;
    params.pd_sep_config.decode_entrance = true;
    params.pd_sep_config.role_type       = RoleType::PDFUSION;
    EXPECT_EQ(service.init(params, nullptr, py::object{}).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

}  // namespace rtp_llm
