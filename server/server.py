import grpc
from concurrent import futures
import time
import uuid
import queue
import threading

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import the generated gRPC classes
from generated import adaptive_sharing_pb2, adaptive_sharing_pb2_grpc

# Thread-safe Queue for request handling
request_queue = queue.Queue()

# Neighbor metrics dictionary
neighbor_metrics = {}

# Configuration: List of neighboring servers
neighbors = ["localhost:50052", "localhost:50053"]  # Replace with actual neighbor addresses


# Server Implementation
class AdaptiveServerServicer(adaptive_sharing_pb2_grpc.AdaptiveServerServicer):
    def __init__(self, server_id):
        self.server_id = server_id

    def HandleRequest(self, request, context):
        """
        Handle incoming client requests.
        Generates a UUID, enqueues the request, and immediately acknowledges the client.
        """
        request_uuid = str(uuid.uuid4())
        print(f"Received Request ID: {request.request_id} | UUID: {request_uuid}")
        request_queue.put((request_uuid, request))
        print(f"Current Queue Size: {request_queue.qsize()}")
        return adaptive_sharing_pb2.ResponseMessage(uuid=request_uuid, status="Enqueued")

    def ShareMetrics(self, request, context):
        """
        Handle metrics received from neighbors.
        Updates the neighbor_metrics dictionary with the latest metrics.
        """
        neighbor_metrics[request.server_id] = {
            "queue_size": request.queue_size,
            "last_updated": time.time()
        }
        print(f"Received Metrics from {request.server_id}: Queue Size {request.queue_size}")
        return adaptive_sharing_pb2.AckMessage(status="Metrics Received")


def process_requests():
    """
    Worker function to process requests asynchronously from the queue.
    """
    while True:
        try:
            # Fetch a request from the queue
            request_uuid, request = request_queue.get(timeout=1)
            print(f"Processing Request ID: {request.request_id} | UUID: {request_uuid}")
            time.sleep(2)  # Simulate processing time
            print(f"Completed Request ID: {request.request_id} | UUID: {request_uuid}")
            request_queue.task_done()
        except queue.Empty:
            continue  # No requests to process, continue checking


def send_metrics(server_id):
    """
    Periodically send the server's metrics (queue size) to all neighbors.
    """
    while True:
        for neighbor in neighbors:
            try:
                with grpc.insecure_channel(neighbor) as channel:
                    stub = adaptive_sharing_pb2_grpc.AdaptiveServerStub(channel)
                    metrics = adaptive_sharing_pb2.MetricsMessage(
                        server_id=server_id,
                        queue_size=request_queue.qsize()
                    )
                    response = stub.ShareMetrics(metrics)
                    print(f"Sent Metrics to {neighbor}: Queue Size {request_queue.qsize()} | Status: {response.status}")
            except Exception as e:
                print(f"Failed to send metrics to {neighbor}: {e}")
        time.sleep(5)  # Send metrics every 5 seconds


def serve():
    """
    Start the gRPC server, worker threads, and metrics-sharing threads.
    """
    server_id = "localhost:50053"  # Change this for each server instance

    # Start worker threads for request processing
    num_workers = 5
    print(f"Starting {num_workers} worker threads...")
    for i in range(num_workers):
        threading.Thread(target=process_requests, daemon=True).start()

    # Start the metrics-sharing thread
    threading.Thread(target=send_metrics, args=(server_id,), daemon=True).start()

    # Start the gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    adaptive_sharing_pb2_grpc.add_AdaptiveServerServicer_to_server(AdaptiveServerServicer(server_id), server)
    server.add_insecure_port(server_id)
    print(f"Server started. Listening on {server_id}...")
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Server shutting down.")


if __name__ == "__main__":
    serve()
