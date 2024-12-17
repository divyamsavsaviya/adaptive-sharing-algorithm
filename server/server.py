import grpc
from concurrent import futures
import time
import uuid
import queue
import threading

from generated import adaptive_sharing_pb2, adaptive_sharing_pb2_grpc

request_queue = queue.Queue()

class AdaptiveServerServicer(adaptive_sharing_pb2_grpc.AdaptiveServerServicer):
    def __init__(self):
        self.queue_size = 0

    def HandleRequest(self, request, context):
        request_uuid = str(uuid.uuid4())
        print(f"Received Request ID: {request.request_id} | UUID: {request_uuid}")
    
        request_queue.put((request_uuid, request))
        self.queue_size = request_queue.qsize()
        print(f"Current Queue Size: {self.queue_size}")

        return adaptive_sharing_pb2.ResponseMessage(uuid=request_uuid, status="Enqueued")

def process_requests():
    """Worker function to process requests from the queue."""
    while True:
        try:
            request_uuid, request = request_queue.get(timeout=1)
            print(f"Processing Request ID: {request.request_id} | UUID: {request_uuid}")
            time.sleep(2)
            
            print(f"Completed Request ID: {request.request_id} | UUID: {request_uuid}")
            request_queue.task_done()
        except queue.Empty:
            continue 

def serve():
    num_workers = 5
    print(f"Starting {num_workers} worker threads...")
    for i in range(num_workers):
        threading.Thread(target=process_requests, daemon=True).start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    adaptive_sharing_pb2_grpc.add_AdaptiveServerServicer_to_server(AdaptiveServerServicer(), server)
    server.add_insecure_port("[::]:50051")
    print("Server started. Listening on port 50051...")
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Server shutting down.")

if __name__ == "__main__":
    serve()
