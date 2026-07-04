import time

from shell.parser import CommandParser


class MiniShell:

    def __init__(self, node):
        self.node = node
        self.parser = CommandParser(node)

    def run(self):

        while True:

            try:

                command_input = input(self.node.get_prompt()).strip()

                if not command_input:
                    continue

                if command_input == "exit":
                    print("Exiting mini-shell.")
                    break

                if command_input.startswith("time "):

                    command_input = command_input[5:]

                    start_time = time.perf_counter()

                    result = self.parser.execute(command_input)

                    end_time = time.perf_counter()

                    if result is not None:
                        print(result)

                    print(f"Execution time: {end_time - start_time:.6f} seconds")

                else:

                    result = self.parser.execute(command_input)

                    if result is not None:
                        print(result)

            except (KeyboardInterrupt, EOFError):
                print("\nExiting mini-shell.")
                break

            except Exception as e:
                print(f"Error: {e}")
