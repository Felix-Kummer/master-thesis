import random
import sys

original_abs = abs
def abs(number):
    return round(original_abs(number), 2)


def run(path = ""):

    with open(path, "w") as file:
        indent = "  "

        runtime_min     = 1       # min 1 second
        runtime_max     = 3600.0  # max 1h runtime
        runtime_std_dev_min = 1       # 1 second
        runtime_std_dev_max = 600     # 10 minutes

        runtime_mean_A  = round(random.uniform(runtime_min, runtime_max), 2)
        runtime_std_dev_A = round(random.uniform(runtime_std_dev_min, runtime_std_dev_max), 2)

        runtime_mean_B  = round(random.uniform(runtime_min, runtime_max), 2)
        runtime_std_dev_B = round(random.uniform(runtime_std_dev_min, runtime_std_dev_max), 2)

        size_min      = 100              # 100 byte
        size_max      = 1074000000       # 1 Gigabyte
        size_std_dev_min = 1            # 1 byte
        size_std_dev_max = 104900000    # 100 Megabyte


        num_jobs = 100



        size_A_in = round(random.uniform(size_min, size_max), 2)

        sizes_A_mean = round(random.uniform(size_min, size_max), 2)
        sizes_A_std_dev = round(random.uniform(size_std_dev_min, size_std_dev_max), 2)
        random_sizes_A_out = [abs(random.gauss(sizes_A_mean, sizes_A_std_dev)) for _ in range(num_jobs)]

        sizes_B_mean = round(random.uniform(size_min, size_max), 2)
        sizes_B_std_dev = round(random.uniform(size_std_dev_min, size_std_dev_max), 2)
        random_sizes_B_out = [abs(random.gauss(sizes_B_mean, sizes_B_std_dev)) for _ in range(num_jobs)]

        file.write(f'<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-2.1.xsd" version="2.1" count="1">\n')


        # init job
        file.write(f'{indent}<job id="ID{0:05d}" namespace="Montage" name="taskA" version="1.0" runtime="{abs(random.gauss(runtime_mean_A, runtime_std_dev_A))}">\n')
        # input
        file.write(f'{indent}{indent}<uses file="input_file" link="input" register="true" transfer="true" optional="false" type="data" size="{size_A_in}"/>\n')
        # outputs
        for i in range(0 ,num_jobs):
            file.write(f'{indent}{indent}<uses file="file{i:03d}" link="output" register="true" transfer="true" optional="false" type="data" size="{random_sizes_A_out[i]}"/>\n')
        # close job scope
        file.write(f'{indent}</job>\n')


        # jobs and files
        for i in range(0, num_jobs):
            # head
            file.write(f'{indent}<job id="ID{i+1:05d}" namespace="Montage" name="taskB" version="1.0" runtime="{abs(random.gauss(runtime_mean_B, runtime_std_dev_B))}">\n')

            # input and output
            file.write(f'{indent}{indent}<uses file="file{i:03d}" link="input" register="true" transfer="true" optional="false" type="data" size="{random_sizes_A_out[i]}"/>\n')
            file.write(f'{indent}{indent}<uses file="file_out{i:03d}" link="output" register="true" transfer="true" optional="false" type="data" size="{random_sizes_B_out[i]}"/>\n')

            # close job scope
            file.write(f'{indent}</job>\n')

        # dependencies
        for i in range(1, num_jobs+1):
            file.write(f'{indent}<child ref="ID{i:05d}">\n')

            file.write(f'{indent}{indent}<parent ref="ID{0:05d}"/>\n')

            file.write(f'{indent}</child>\n')

        file.write('</adag>\n')
