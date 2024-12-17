import grpc
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from generated import adaptive_sharing_pb2
from generated import adaptive_sharing_pb2_grpc

def run():
    # Connect to the server
    with grpc.insecure_channel("localhost:50053") as channel:
        stub = adaptive_sharing_pb2_grpc.AdaptiveServerStub(channel)
        
        # Send a sample request
        for i in range(200):
            request_id = f"Request-{i}"
            print(f"Sending Request: {request_id}")
            response = stub.HandleRequest(adaptive_sharing_pb2.RequestMessage(request_id=request_id, payload="Test Payload"))
            print(f"Response - UUID: {response.uuid}, Status: {response.status}")

if __name__ == "__main__":
    run()
