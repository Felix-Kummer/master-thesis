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

        size_min      = 100              # 100 byte
        size_max      = 1074000000       # 1 Gigabyte

        size_std_dev_min = 1            # 1 byte
        size_std_dev_max = 104900000    # 100 Megabyte

        num_jobs_per_pipeline = 3
        num_pipelines         = 100


        size_means = [round(random.uniform(size_min, size_max), 2) for _ in range(num_jobs_per_pipeline + 1)]
        size_std_devs = [round(random.uniform(size_std_dev_min, size_std_dev_max), 2) for _ in range(num_jobs_per_pipeline + 1)]

        random_sizes = [
            [abs(random.gauss(size_means[x], size_std_devs[x])) for x in range(num_jobs_per_pipeline + 1) ]
            for _ in range(num_pipelines)
        ]

        runtime_means = [round(random.uniform(runtime_min, runtime_max), 2) for _ in range(num_jobs_per_pipeline)]
        runtime_std_devs = [round(random.uniform(runtime_std_dev_min, runtime_std_dev_max), 2) for _ in range(num_jobs_per_pipeline)]

        file.write(f'<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-2.1.xsd" version="2.1" count="1">\n')


        dependency_str = ""

        for k in range(0, num_pipelines * num_jobs_per_pipeline):

            group_number = k // num_jobs_per_pipeline

            position_in_group = k % num_jobs_per_pipeline

            # head
            file.write(f'{indent}<job id="ID{k:05d}" namespace="Montage" name="task{position_in_group}" version="1.0" runtime="{abs(random.gauss(runtime_means[position_in_group], runtime_std_devs[position_in_group]))}">\n')

            # input
            file.write(f'{indent}{indent}<uses file="file{k:03d}" link="input" register="true" transfer="true" optional="false" type="data" size="{random_sizes[group_number][position_in_group]}"/>\n')


            # group end
            if (k+1) % num_jobs_per_pipeline == 0:

                # output
                file.write(f'{indent}{indent}<uses file="outputFileGroup{group_number:03d}" link="output" register="true" transfer="true" optional="false" type="data" size="{random_sizes[group_number][position_in_group+1]}"/>\n')

                # close job scope
                file.write(f'{indent}</job>\n')

                # signal group end
                file.write(f'<!-- Group {group_number} finished --> \n')

                # dependencies
                dependency_str += f'{indent}<child ref="ID{k:05d}">\n'
                dependency_str += f'{indent}{indent}<parent ref="ID{k-1:05d}"/>\n'
                dependency_str += f'{indent}</child>\n'

            else:
                # output
                file.write(f'{indent}{indent}<uses file="file{k+1:03d}" link="output" register="true" transfer="true" optional="false" type="data" size="{random_sizes[group_number][position_in_group+1]}"/>\n')
                # close job scope
                file.write(f'{indent}</job>\n')

                # dependencies
                if k % num_jobs_per_pipeline == 0:
                    # start job -> no parent
                    pass
                else:
                    dependency_str += f'{indent}<child ref="ID{k:05d}">\n'
                    dependency_str += f'{indent}{indent}<parent ref="ID{k-1:05d}"/>\n'
                    dependency_str += f'{indent}</child>\n'

        file.write(dependency_str)
        file.write('</adag>\n')
