import subprocess
import sys


def install_requirements(requirements_file="requirements.txt"):
    try:
        # Run the pip install command
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-r", requirements_file]
        )
        print(
            f"All requirements from {requirements_file} have been installed successfully."
        )
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while installing the requirements: {e}")


# Example usage
install_requirements("requirements.txt")
