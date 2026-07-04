import os
import shutil


class FileManager:

    @staticmethod
    def mkdir(command_input, scratch_dir=None):

        name = command_input.split()[1]
        if scratch_dir:
            name = os.path.join(scratch_dir, name)

        os.makedirs(name, exist_ok=True)

    @staticmethod
    def rmdir(command_input, scratch_dir=None):

        name = command_input.split()[1]
        if scratch_dir:
            name = os.path.join(scratch_dir, name)

        os.rmdir(name)

    @staticmethod
    def remove_recursively(command_input, scratch_dir=None):

        directory = command_input.split()[2]
        if scratch_dir:
            directory = os.path.join(scratch_dir, directory)

        shutil.rmtree(directory)

    @staticmethod
    def cd(command_input):

        directory = command_input.split()[1]

        os.chdir(directory)

    @staticmethod
    def cp(command_input, scratch_dir=None):

        _, source, destination = command_input.split()
        if scratch_dir:
            # Try to read from scratch first, fallback to real file
            src_tentative = os.path.join(scratch_dir, source)
            if os.path.exists(src_tentative):
                source = src_tentative
            destination = os.path.join(scratch_dir, destination)

        shutil.copy2(source, destination)

    @staticmethod
    def echo(command_input, scratch_dir=None):

        content, file = command_input.split(">")

        text = content.replace("echo", "", 1).strip().strip('"')
        file_path = file.strip()

        if scratch_dir:
            file_path = os.path.join(scratch_dir, file_path)

        with open(file_path, "w") as f:
            f.write(text)

