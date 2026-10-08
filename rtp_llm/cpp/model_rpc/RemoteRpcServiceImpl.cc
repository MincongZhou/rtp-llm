#include "rtp_llm/cpp/model_rpc/RemoteRpcServiceImpl.h"

namespace rtp_llm {

grpc::Status RemoteRpcServiceImpl::init(const EngineInitParams&                                params,
                                        std::unique_ptr<rtp_llm::ProposeModelEngineInitParams> propose_params,
                                        py::object                                             mm_process_engine) {
    if (!params.pd_sep_config.decode_entrance) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                            "Legacy PD separation has been removed; set decode_entrance=true");
    }
    if (params.pd_sep_config.role_type == RoleType::PREFILL) {
        prefill_server_new2_ = std::make_shared<PrefillRpcServerNew2>();
        local_server_        = prefill_server_new2_;
        return prefill_server_new2_->init(params, std::move(propose_params), mm_process_engine);
    }
    if (params.pd_sep_config.role_type == RoleType::DECODE) {
        decode_server_new2_ = std::make_shared<DecodeRpcServerNew2>();
        local_server_       = decode_server_new2_;
        return decode_server_new2_->init(params, std::move(propose_params), mm_process_engine);
    }
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "PD server requires Prefill or Decode role");
}

}  // namespace rtp_llm
