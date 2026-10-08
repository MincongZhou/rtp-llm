#pragma once
#include <memory>
#include "rtp_llm/cpp/model_rpc/LocalRpcServiceImpl.h"
#include "rtp_llm/cpp/model_rpc/PrefillRpcServerNew2.h"
#include "rtp_llm/cpp/model_rpc/DecodeRpcServerNew2.h"

namespace rtp_llm {

class RemoteRpcServiceImpl: public LocalRpcServiceImpl {
public:
    grpc::Status init(const EngineInitParams&                                params,
                      std::unique_ptr<rtp_llm::ProposeModelEngineInitParams> propose_params,
                      py::object                                             mm_process_engine) override;

    grpc::Status GenerateStreamCall(grpc::ServerContext*                   context,
                                    const GenerateInputPB*                 request,
                                    grpc::ServerWriter<GenerateOutputsPB>* writer) override {
        if (decode_server_new2_) {
            return decode_server_new2_->GenerateStreamCall(context, request, writer);
        }
        if (prefill_server_new2_) {
            return prefill_server_new2_->GenerateStreamCall(context, request, writer);
        }
        return grpc::Status(grpc::StatusCode::INTERNAL, "PD server is not initialized");
    }

    grpc::Status
    BatchGenerateCall(grpc::ServerContext*, const BatchGenerateInputPB*, BatchGenerateOutputsPB*) override {
        return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "/batch_infer is not supported with decode_entrance");
    }

    grpc::Status EnqueueBatch(grpc::ServerContext*         context,
                              const EnqueueBatchRequestPB* request,
                              EnqueueBatchResponsePB*      response) override {
        if (prefill_server_new2_) {
            return prefill_server_new2_->EnqueueBatch(context, request, response);
        }
        return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "EnqueueBatch requires Prefill role");
    }

    grpc::Status StartLoad(grpc::ServerContext*                  context,
                           const P2PConnectorStartLoadRequestPB* request,
                           P2PConnectorStartLoadResponsePB*      response) override {
        if (prefill_server_new2_) {
            return prefill_server_new2_->StartLoad(context, request, response);
        }
        return grpc::Status(grpc::StatusCode::INTERNAL, "server not implement StartLoad");
    }

    grpc::Status GetPeerInfo(grpc::ServerContext*        context,
                             const GetPeerInfoRequestPB* request,
                             GetPeerInfoResponsePB*      response) override {
        if (prefill_server_new2_) {
            return prefill_server_new2_->GetPeerInfo(context, request, response);
        }
        return grpc::Status(grpc::StatusCode::INTERNAL, "server not implement GetPeerInfo");
    }

    void stop() override {
        if (prefill_server_new2_) {
            prefill_server_new2_->stop();
        }
        if (decode_server_new2_) {
            decode_server_new2_->stop();
        }
    }

private:
    std::shared_ptr<PrefillRpcServerNew2> prefill_server_new2_;
    std::shared_ptr<DecodeRpcServerNew2>  decode_server_new2_;
};

}  // namespace rtp_llm
