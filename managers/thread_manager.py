import os
import shutil
import threading


class ThreadManager:

    @staticmethod
    def backup_dir(directory):

        backup_dir = directory + "_backup"

        os.makedirs(backup_dir, exist_ok=True)

        for item in os.listdir(directory):
            source = os.path.join(directory, item)

            if os.path.isfile(source):
                shutil.copy2(source, backup_dir)

        print(f"Backup completed: {backup_dir}")

    @staticmethod
    def backup(command_input):

        directory = command_input.split()[1]

        thread = threading.Thread(
            target=ThreadManager.backup_dir,
            args=(directory,)
        )

        thread.start()

        print(f"Backup started for directory: {directory}")

    @staticmethod
    def test():

        print("Starting thread test to demonstrate thread management")

        def worker():
            print("Worker thread is exiting")

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()
