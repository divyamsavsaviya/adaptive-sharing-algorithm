import grpc
from concurrent import futures
import time
import queue
import threading
import psutil
import uuid
import argparse
import csv

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from generated import adaptive_sharing_pb2, adaptive_sharing_pb2_grpc


# Constants
METRICS_INTERVAL = 2       # Time interval for sharing metrics (in seconds)
WINDOW_SIZE = 10           # Sliding window size for incoming request rate

# Dynamic Weights Initialization
W_QUEUE = 0.4
W_CPU = 0.3
W_MEMORY = 0.2
W_RATE = 0.1


# Adaptive Sharing Server Implementation
class AdaptiveServerServicer(adaptive_sharing_pb2_grpc.AdaptiveServerServicer):
    def __init__(self, server_id, neighbors):
        self.server_id = server_id
        self.neighbors = neighbors
        self.request_queue = queue.Queue()
        self.neighbor_metrics = {}
        self.incoming_request_times = []
        self.lock = threading.Lock()
        self.csv_file = f"server_metrics_{server_id.replace(':', '_')}.csv"

        # Write CSV Header if file does not exist
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([
                    "Request ID", "Server ID", "Load Score", "Avg Neighbor Load",
                    "Forwarded", "Selected Neighbor", "Queue Size", "CPU Usage", "Memory Usage"
                ])

    def calculate_load_score(self, queue_size, cpu_usage, memory_usage, request_rate):
        """Combine real-time metrics into a single adaptive load score."""
        return W_QUEUE * queue_size + W_CPU * cpu_usage + W_MEMORY * memory_usage + W_RATE * request_rate

    def adjust_weights(self, queue_size, cpu_usage, memory_usage, request_rate):
        """Dynamically adjust weights based on the dominance of metrics."""
        global W_QUEUE, W_CPU, W_MEMORY, W_RATE
        total = queue_size + cpu_usage + memory_usage + request_rate
        if total == 0: return  # Prevent division by zero
        W_QUEUE = queue_size / total
        W_CPU = cpu_usage / total
        W_MEMORY = memory_usage / total
        W_RATE = request_rate / total

    def compute_request_rate(self):
        """Calculate incoming request rate using a sliding window."""
        current_time = time.time()
        self.incoming_request_times.append(current_time)
        self.incoming_request_times = [t for t in self.incoming_request_times if t > current_time - WINDOW_SIZE]
        return len(self.incoming_request_times) / WINDOW_SIZE

    def select_least_loaded_neighbor(self):
        """Select the neighbor with the lowest load score."""
        min_load = float('inf')
        best_neighbor = None
        for neighbor, metrics in self.neighbor_metrics.items():
            load = self.calculate_load_score(metrics['queue_size'], metrics['cpu_usage'], metrics['memory_usage'], 0)
            if load < min_load:
                min_load = load
                best_neighbor = neighbor
        return best_neighbor

    def calculate_average_neighbor_load(self):
        """Calculate the average load score across all neighbors."""
        total_load = 0
        count = len(self.neighbor_metrics)
        for metrics in self.neighbor_metrics.values():
            total_load += self.calculate_load_score(metrics['queue_size'], metrics['cpu_usage'], metrics['memory_usage'], 0)
        return total_load / count if count > 0 else 0

    def HandleRequest(self, request, context):
        """gRPC method to handle incoming requests."""
        request_id = str(uuid.uuid4())
        queue_size = self.request_queue.qsize()
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        request_rate = self.compute_request_rate()

        # Adjust weights dynamically
        self.adjust_weights(queue_size, cpu_usage, memory_usage, request_rate)

        # Calculate load score
        current_load = self.calculate_load_score(queue_size, cpu_usage, memory_usage, request_rate)
        average_load = self.calculate_average_neighbor_load()

        forwarded = False
        selected_neighbor = None

        # Forward request if load exceeds threshold
        if current_load > 1.5 * average_load:
            selected_neighbor = self.select_least_loaded_neighbor()
            if selected_neighbor:
                print(f"Forwarding request {request.request_id} to {selected_neighbor}")
                try:
                    with grpc.insecure_channel(selected_neighbor) as channel:
                        stub = adaptive_sharing_pb2_grpc.AdaptiveServerStub(channel)
                        stub.HandleRequest(request)
                        forwarded = True
                        self.log_metrics(request.request_id, current_load, average_load, forwarded,
                                         selected_neighbor, queue_size, cpu_usage, memory_usage)
                        return adaptive_sharing_pb2.ResponseMessage(status="Forwarded")
                except Exception as e:
                    print(f"Forwarding failed: {e}")

        # Process locally
        print(f"Processing request {request.request_id} locally")
        self.request_queue.put(request_id)
        time.sleep(0.5)  # Simulate processing time

        # Log metrics
        self.log_metrics(request.request_id, current_load, average_load, forwarded,
                         selected_neighbor, queue_size, cpu_usage, memory_usage)
        return adaptive_sharing_pb2.ResponseMessage(status="Processed")
    
    
    def log_metrics(self, request_id, load_score, avg_load, forwarded,
                    selected_neighbor, queue_size, cpu_usage, memory_usage):
        """Log metrics to a CSV file."""
        with open(self.csv_file, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                request_id, self.server_id, round(load_score, 2),
                round(avg_load, 2), forwarded, selected_neighbor or "N/A",
                queue_size, round(cpu_usage, 2), round(memory_usage, 2)
            ])

    def SendMetrics(self, request, context):
        """Receive metrics from neighbors."""
        print(f"Received metrics from {request.server_id}")
        with self.lock:
            self.neighbor_metrics[request.server_id] = {
                "queue_size": request.queue_size,
                "cpu_usage": request.cpu_usage,
                "memory_usage": request.memory_usage
            }
        return adaptive_sharing_pb2.ResponseMessage(status="Metrics Received")

    def send_metrics_to_neighbors(self):
        """Periodically share metrics with neighbors."""
        while True:
            # Collect current metrics
            metrics = {
                "queue_size": self.request_queue.qsize(),
                "cpu_usage": psutil.cpu_percent(),
                "memory_usage": psutil.virtual_memory().percent,
            }
            
            for neighbor in self.neighbors:
                try:
                    # Establish a connection with the neighbor
                    with grpc.insecure_channel(neighbor) as channel:
                        stub = adaptive_sharing_pb2_grpc.AdaptiveServerStub(channel)
                        
                        # Test the connection first (optional but improves reliability)
                        grpc.channel_ready_future(channel).result(timeout=2)
                        
                        # Send the metrics message
                        stub.SendMetrics(adaptive_sharing_pb2.MetricsMessage(
                            server_id=self.server_id,
                            queue_size=metrics["queue_size"],
                            cpu_usage=metrics["cpu_usage"],
                            memory_usage=metrics["memory_usage"]
                        ))
                        print(f"[SUCCESS] Sent metrics to {neighbor}")
                except grpc.FutureTimeoutError:
                    print(f"[TIMEOUT] Failed to connect to {neighbor} (server not ready)")
                except Exception as e:
                    print(f"[ERROR] Failed to send metrics to {neighbor}: {e}")

            time.sleep(METRICS_INTERVAL)

def check_neighbor_connections(neighbors):
    """Test connectivity to all neighbors."""
    for neighbor in neighbors:
        try:
            with grpc.insecure_channel(neighbor) as channel:
                grpc.channel_ready_future(channel).result(timeout=2)
                print(f"[CONNECTED] Successfully connected to neighbor {neighbor}")
        except grpc.FutureTimeoutError:
            print(f"[WARNING] Neighbor {neighbor} is not ready (timeout)")
        except Exception as e:
            print(f"[ERROR] Could not connect to neighbor {neighbor}: {e}")

def serve(port, neighbors):
    """Start the gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = AdaptiveServerServicer(f"localhost:{port}", neighbors)
    adaptive_sharing_pb2_grpc.add_AdaptiveServerServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    print(f"Server started on port {port}")

    if neighbors:
        check_neighbor_connections(neighbors)

    # Start metrics sharing in a background thread
    threading.Thread(target=servicer.send_metrics_to_neighbors, daemon=True).start()

    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start an Adaptive Sharing Server")
    parser.add_argument("--port", type=int, required=True, help="Port number for the server")
    parser.add_argument("--neighbors", nargs="*", help="List of neighbor server addresses (e.g., localhost:50052 localhost:50053)")

    args = parser.parse_args()
    serve(args.port, args.neighbors if args.neighbors else [])
