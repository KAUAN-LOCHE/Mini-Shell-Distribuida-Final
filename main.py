import argparse

from shell.prompt import MiniShell
from network.node import Node

parser = argparse.ArgumentParser(description="MiniShell Linux Distribuida")

parser.add_argument("--id", required=True, type=int, help="Numeric node identifier")
parser.add_argument("--port", required=True, type=int, help="TCP port for this node")
parser.add_argument("--host", default="127.0.0.1", help="Host/IP to bind to")

args = parser.parse_args()

node = Node(node_id=args.id, port=args.port, host=args.host)

node.start()

shell = MiniShell(node)

shell.run()
