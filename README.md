# Running the Adaptive Sharding Project

## Prerequisites

- Python 3.6 or higher
- pip package manager

## Installation

1. Unzip the project archive:

   ```bash
   unzip adaptive_sharding_project.zip
   ```

2. Change to the project directory:

   ```bash
   cd adaptive_sharding_project
   ```

3. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Running the Servers

1. Start each server on a separate terminal, specifying the port and neighbor addresses:

   ```bash
   python server/server.py --port 50051 --neighbors localhost:50052 localhost:50053
   python server/server.py --port 50052 --neighbors localhost:50051 localhost:50053 
   python server/server.py --port 50053 --neighbors localhost:50051 localhost:50052
   ```

   Adjust the port numbers and neighbor addresses as needed for your setup.

2. The servers will start and begin sharing metrics with each other.

## Running the Client

1. In a new terminal, run the client script:

   ```bash
   python client/client.py
   ```

   The client will send a series of requests to the server cluster, and the adaptive sharding algorithm will distribute the requests among the servers.

2. Monitor the server terminals to see the request handling and forwarding behavior.
