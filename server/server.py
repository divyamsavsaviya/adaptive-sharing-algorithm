import grpc
from concurrent import futures
import time
import uuid

from generated import adaptive_sharing_pb2, adaptive_sharing_pb2_grpc

class AdaptiveServerServicer(adaptive_sharing_pb2_grpc.AdaptiveServerServicer):
    def HandleRequest(self, request, context):
        request_uuid = str(uuid.uuid4())
        print(f"Received Request ID: {request.request_id} | UUID: {request_uuid}")
        return adaptive_sharing_pb2.ResponseMessage(uuid=request_uuid, status="Processed")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    adaptive_sharing_pb2_grpc.add_AdaptiveServerServicer_to_server(AdaptiveServerServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("Server started. Listening on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
