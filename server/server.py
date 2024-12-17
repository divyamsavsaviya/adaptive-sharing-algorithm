import grpc
from concurrent import futures
import time
import uuid
import queue
import threading
import psutil
import argparse 

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import the generated gRPC classes
from generated import adaptive_sharing_pb2
from generated import adaptive_sharing_pb2_grpc

# Thread-safe Queue for request handling
request_queue = queue.Queue()

# Neighbor metrics dictionary
neighbor_metrics = {}

# Configuration: List of neighboring servers
neighbors = ["localhost:50052", "localhost:50053"]  # Replace with actual neighbor addresses

# Weights for load calculation
W_QUEUE = 0.5     # Weight for queue size
W_CPU = 0.3       # Weight for CPU usage
W_MEMORY = 0.2    # Weight for memory usage

# Server Implementation
class AdaptiveServerServicer(adaptive_sharing_pb2_grpc.AdaptiveServerServicer):
    def __init__(self, server_id):
        self.server_id = server_id

    def HandleRequest(self, request, context):
        request_uuid = str(uuid.uuid4())
        print(f"Received Request ID: {request.request_id} | UUID: {request_uuid}")

        current_load_score = self.calculate_load_score()
        average_load_score = self.calculate_average_load_score()

        # Adaptive forwarding logic
        if current_load_score > 1.5 * average_load_score:
            least_loaded_neighbor = self.get_least_loaded_neighbor()
            if least_loaded_neighbor:
                print(f"Forwarding Request ID: {request.request_id} to {least_loaded_neighbor}")
                try:
                    with grpc.insecure_channel(least_loaded_neighbor) as channel:
                        stub = adaptive_sharing_pb2_grpc.AdaptiveServerStub(channel)
                        response = stub.HandleRequest(request)
                        print(f"Request ID: {request.request_id} successfully forwarded to {least_loaded_neighbor}")
                        return response
                except Exception as e:
                    print(f"Failed to forward Request ID: {request.request_id} to {least_loaded_neighbor}: {e}")
        
        request_queue.put((request_uuid, request))
        print(f"Enqueued Request ID: {request.request_id} | UUID: {request_uuid} | Queue Size: {request_queue.qsize()}")
        return adaptive_sharing_pb2.ResponseMessage(uuid=request_uuid, status="Enqueued")

    def ShareMetrics(self, request, context):
        neighbor_metrics[request.server_id] = {
            "queue_size": request.queue_size,
            "cpu_usage": request.cpu_usage,
            "memory_usage": request.memory_usage,
            "last_updated": time.time()
        }
        print(f"Received Metrics from {request.server_id}: Queue Size {request.queue_size}, CPU {request.cpu_usage}%, Memory {request.memory_usage}%")
        return adaptive_sharing_pb2.AckMessage(status="Metrics Received")

    def get_least_loaded_neighbor(self):
        least_loaded = None
        min_load_score = float('inf')
        for neighbor, metrics in neighbor_metrics.items():
            load_score = self.calculate_combined_load(metrics['queue_size'], metrics['cpu_usage'], metrics['memory_usage'])
            if load_score < min_load_score:
                least_loaded = neighbor
                min_load_score = load_score
        return least_loaded

    def calculate_load_score(self):
        queue_size = request_queue.qsize()
        cpu_usage = psutil.cpu_percent(interval=0.1)
        memory_usage = psutil.virtual_memory().percent
        return self.calculate_combined_load(queue_size, cpu_usage, memory_usage)

    @staticmethod
    def calculate_combined_load(queue_size, cpu_usage, memory_usage):
        return W_QUEUE * queue_size + W_CPU * cpu_usage + W_MEMORY * memory_usage

    def calculate_average_load_score(self):
        total_score = self.calculate_load_score()
        num_servers = 1
        for metrics in neighbor_metrics.values():
            total_score += self.calculate_combined_load(metrics['queue_size'], metrics['cpu_usage'], metrics['memory_usage'])
            num_servers += 1
        return total_score / num_servers


def process_requests():
    while True:
        try:
            request_uuid, request = request_queue.get(timeout=1)
            print(f"Processing Request ID: {request.request_id} | UUID: {request_uuid}")
            time.sleep(10)  # Simulate processing time
            print(f"Completed Request ID: {request.request_id} | UUID: {request_uuid}")
            request_queue.task_done()
        except queue.Empty:
            continue


def send_metrics(server_id, neighbors):
    while True:
        for neighbor in neighbors:
            try:
                with grpc.insecure_channel(neighbor) as channel:
                    stub = adaptive_sharing_pb2_grpc.AdaptiveServerStub(channel)
                    metrics = adaptive_sharing_pb2.MetricsMessage(
                        server_id=server_id,
                        queue_size=request_queue.qsize(),
                        cpu_usage=psutil.cpu_percent(interval=0.1),
                        memory_usage=psutil.virtual_memory().percent
                    )
                    response = stub.ShareMetrics(metrics)
                    print(f"Sent Metrics to {neighbor}: Queue Size {metrics.queue_size}, CPU {metrics.cpu_usage}%, Memory {metrics.memory_usage}% | Status: {response.status}")
            except Exception as e:
                print(f"Failed to send metrics to {neighbor}: {e}")
        time.sleep(5)


def serve(port, neighbors):
    server_id = f"localhost:{port}"
    print(f"Starting server with ID: {server_id}")

    # Start worker threads
    num_workers = 5
    print(f"Starting {num_workers} worker threads...")
    for _ in range(num_workers):
        threading.Thread(target=process_requests, daemon=True).start()

    # Start the metrics-sharing thread
    threading.Thread(target=send_metrics, args=(server_id, neighbors), daemon=True).start()

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
    parser = argparse.ArgumentParser(description="Start an Adaptive Sharing Server")
    parser.add_argument("--port", type=int, required=True, help="Port number for the server")
    parser.add_argument("--neighbors", nargs="*", help="List of neighbor server addresses (e.g., localhost:50052 localhost:50053)")

    args = parser.parse_args()

    # Default to an empty list if neighbors are not provided
    neighbors = args.neighbors if args.neighbors else []
    serve(args.port, neighbors)
