import subprocess


def run_docker_container(container_name, command):
    try:
        subprocess.run(
            f"docker run --rm {container_name} {command}", shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running container {container_name}: {e}")


# Extraction step
run_docker_container("extractor-container")

# Loading step
run_docker_container("loader-container")
