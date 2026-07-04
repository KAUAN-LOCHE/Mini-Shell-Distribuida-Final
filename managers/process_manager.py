import os


class ProcessManager:

    @staticmethod
    def execute(command_input):

        args = command_input.split()
        pid = os.fork()

        if pid == 0:
            try:
                os.execvp(args[0], args)

            except FileNotFoundError:
                print(f"Command not found: {args[0]}")
                os._exit(1)

        else:
            os.wait()

    @staticmethod
    def test():

        print("Starting process test to demonstrate process management")
        pid = os.fork()

        if pid == 0:
            os._exit(0)

        else:
            os.wait()
