import random
import sys

original_abs = abs
def abs(number):
    return round(original_abs(number), 2)

def run(path = ""):


    with open(path, "w") as file:
        indent = "  "

        runtime_min = 1       # min 1 second
        runtime_max = 3600.0  # max 1h runtime

        size_min = 100              # 100 byte
        size_max = 1074000000       # 1 Gigabyte

        num_jobs = 100

        random_sizes = [round(random.uniform(size_min, size_max), 2) for _ in range(num_jobs+1)]



        file.write(f'<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-2.1.xsd" version="2.1" count="1">\n')

        # jobs and files
        for i in range(0, num_jobs):
            # head
            file.write(f'{indent}<job id="ID{i:05d}" namespace="Montage" name="task{i:03d}" version="1.0" runtime="{round(random.uniform(runtime_min, runtime_max), 2)}">\n')

            # input and output
            file.write(f'{indent}{indent}<uses file="file{i:03d}" link="input" register="true" transfer="true" optional="false" type="data" size="{random_sizes[i]}"/>\n')
            file.write(f'{indent}{indent}<uses file="file{i+1:03d}" link="output" register="true" transfer="true" optional="false" type="data" size="{random_sizes[i+1]}"/>\n')

            # close job scope
            file.write(f'{indent}</job>\n')

        # dependencies
        for i in range(1, num_jobs):
            file.write(f'{indent}<child ref="ID{i:05d}">\n')

            file.write(f'{indent}{indent}<parent ref="ID{i-1:05d}"/>\n')

            file.write(f'{indent}</child>\n')

        file.write('</adag>\n')
