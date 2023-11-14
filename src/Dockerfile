FROM python:3.9

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY .. .

# Run your script when the container launches
CMD ["python", "./data_processor_multi.py"]
# docker run --name my-running-app -v /path/to/your_dataset.psv:/usr/src/app/your_dataset.psv my-python-app

