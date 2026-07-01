import os
import shutil


class FileManager:

    @staticmethod
    def mkdir(command_input):

        name = command_input.split()[1]

        os.makedirs(name, exist_ok=True)

    @staticmethod
    def rmdir(command_input):

        name = command_input.split()[1]

        os.rmdir(name)

    @staticmethod
    def remove_recursively(command_input):

        directory = command_input.split()[2]

        shutil.rmtree(directory)

    @staticmethod
    def cd(command_input):

        directory = command_input.split()[1]

        os.chdir(directory)

    @staticmethod
    def cp(command_input):

        _, source, destination = command_input.split()

        shutil.copy2(source, destination)

    @staticmethod
    def echo(command_input):

        content, file = command_input.split(">")

        text = content.replace("echo", "", 1).strip().strip('"')

        with open(file.strip(), "w") as f:
            f.write(text)
