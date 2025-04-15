import random
import sys

original_abs = abs
def abs(number):
    return round(original_abs(number), 2)

def run(path = ""):

    with open(path, "w") as file:
        indent = "  "

        runtime_min         = 1       # min 1 second
        runtime_max         = 3600.0  # max 1h runtime
        runtime_std_dev_min = 1       # 1 second
        runtime_std_dev_max = 600     # 10 minutes


        runtime_mean_A  = round(random.uniform(runtime_min, runtime_max), 2)
        runtime_std_dev_A = round(random.uniform(runtime_std_dev_min, runtime_std_dev_max), 2)

        runtime_mean_B  = round(random.uniform(runtime_min, runtime_max), 2)
        runtime_std_dev_B = round(random.uniform(runtime_std_dev_min, runtime_std_dev_max), 2)

        size_min         = 100          # 100 byte
        size_max         = 1074000000   # 1 Gigabyte

        size_std_dev_min = 1            # 1 byte
        size_std_dev_max = 104900000    # 100 Megabyte

        num_jobs = 100

        input_sizes_mean = round(random.uniform(size_min, size_max), 2)
        input_sizes_std_dev = round(random.uniform(size_std_dev_min, size_std_dev_max), 2)
        random_sizes_input = [abs(random.gauss(input_sizes_mean, input_sizes_std_dev)) for _ in range(num_jobs)]

        output_sizes_mean = round(random.uniform(size_min, size_max), 2)
        output_sizes_std_dev = round(random.uniform(size_std_dev_min, size_std_dev_max), 2)
        random_sizes_output = [abs(random.gauss(output_sizes_mean, output_sizes_std_dev)) for _ in range(num_jobs)]

        b_output_size = round(random.uniform(size_min, size_max), 2)




        file.write(f'<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-2.1.xsd" version="2.1" count="1">\n')




        # jobs and files
        for i in range(0, num_jobs):
            # head
            file.write(f'{indent}<job id="ID{i:05d}" namespace="Montage" name="taskA" version="1.0" runtime="{abs(random.gauss(runtime_mean_A, runtime_std_dev_A))}">\n')

            # input and output
            file.write(f'{indent}{indent}<uses file="file_input{i:03d}" link="input" register="true" transfer="true" optional="false" type="data" size="{random_sizes_input[i]}"/>\n')
            file.write(f'{indent}{indent}<uses file="file_output{i:03d}" link="output" register="true" transfer="true" optional="false" type="data" size="{random_sizes_output[i]}"/>\n')

            # close job scope
            file.write(f'{indent}</job>\n')

        # aggregator job
        file.write(f'{indent}<job id="ID{num_jobs:05d}" namespace="Montage" name="taskB" version="1.0" runtime="{abs(random.gauss(runtime_mean_B, runtime_std_dev_B))}">\n')

        for i in range(0, num_jobs):
            file.write(f'{indent}{indent}<uses file="file_output{i:03d}" link="input" register="true" transfer="true" optional="false" type="data" size="{random_sizes_output[i]}"/>\n')

        file.write(f'{indent}{indent}<uses file="file{num_jobs:03d}" link="output" register="true" transfer="true" optional="false" type="data" size="{b_output_size}"/>\n')
        file.write(f'{indent}</job>\n')

        # dependencies
        file.write(f'{indent}<child ref="ID{num_jobs:05d}">\n')
        for i in range(0, num_jobs):
            file.write(f'{indent}{indent}<parent ref="ID{i:05d}"/>\n')
        file.write(f'{indent}</child>\n')



        file.write('</adag>\n')
