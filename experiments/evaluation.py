import copy
import os
import random
import sys
import subprocess
import csv
import json

SYNTHETIC_WFS = 5
NUM_DISTRIBUTIONS = 5

T = 0
N = 0

RND_REPS = 30

def generate_synthetic_workflows():
    wfs = list()

    for i in range(SYNTHETIC_WFS):
        wf = dict()
        wf['name'] = f"aggregation{i}"
        path = f"{os.getcwd()}/evaluation_synthetic_wfs/{wf['name']}.xml"
        wf['path'] = path
        aggregation.run(str(path))
        wfs.append(wf)

        wf = dict()
        wf['name'] = f"distribution{i}"
        path = f"{os.getcwd()}/evaluation_synthetic_wfs/{wf['name']}.xml"
        wf['path'] = path
        distribution.run(str(path))
        wfs.append(wf)

        wf = dict()
        wf['name'] = f"groups{i}"
        path = f"{os.getcwd()}/evaluation_synthetic_wfs/{wf['name']}.xml"
        wf['path'] = path
        groups.run(str(path))
        wfs.append(wf)

        wf = dict()
        wf['name'] = f"longPipeline{i}"
        path = f"{os.getcwd()}/evaluation_synthetic_wfs/{wf['name']}.xml"
        wf['path'] = path
        longPipeline.run(str(path))
        wfs.append(wf)

        wf = dict()
        wf['name'] = f"multiPipeline{i}"
        path = f"{os.getcwd()}/evaluation_synthetic_wfs/{wf['name']}.xml"
        wf['path'] = path
        multiPipeline.run(str(path))
        wfs.append(wf)

        wf = dict()
        wf['name'] = f"redistribution{i}"
        path = f"{os.getcwd()}/evaluation_synthetic_wfs/{wf['name']}.xml"
        wf['path'] = path
        redistribution.run(str(path))
        wfs.append(wf)

    return wfs

def get_real_workflows():
    wfs = list()

    wf = dict()
    wf['name'] = "CyberShake_30"
    wf['path'] = '../../workflowSim_git/config/dax/CyberShake_30.xml'
    wfs.append(wf)

    wf = dict()
    wf['name'] = "CyberShake_50"
    wf['path'] = '../../workflowSim_git/config/dax/CyberShake_50.xml'
    wfs.append(wf)

    wf = dict()
    wf['name'] = "CyberShake_100"
    wf['path'] = '../../workflowSim_git/config/dax/CyberShake_100.xml'
    wfs.append(wf)

    wf = dict()
    wf['name'] = "CyberShake_1000"
    wf['path'] = '../../workflowSim_git/config/dax/CyberShake_1000.xml'
    wfs.append(wf)

    wf = dict()
    wf['name'] = "Epigenomics_24"
    wf['path'] = '../../workflowSim_git/config/dax/Epigenomics_24.xml'
    wfs.append(wf)

    wf = dict()
    wf['name'] = "floodplain"
    wf['path'] = '../../workflowSim_git/config/dax/floodplain.xml'
    wfs.append(wf)

    wf = dict()
    wf['name'] = "Sipht_30.xml"
    wf['path'] = '../../workflowSim_git/config/dax/Sipht_30.xml'
    wfs.append(wf)

    return wfs


def get_files_and_size(wf):
    # get input files and sizes
    csv_path = f'{os.getcwd()}/evaluation_inputs/{wf["name"]}.csv'
    result = subprocess.run(
        ['java', '-cp', java_cp, 'federatedSim.getInputFilesHelper' , f'{wf["path"]}', csv_path],
        capture_output=True,
        text=True,
        env=env
    )
    # print(result.stdout)
    # print(result.stderr)

    # get input files and their sizes, and the total size of the workflow
    total_file_size = 0
    file2size = dict()
    with open(csv_path, mode='r') as file:
        reader = csv.reader(file)
        total_file_size = int(next(reader)[1])  # first line contains total size
        input_size = int(next(reader)[1])  # second line contains total input size

        for row in reader:
            if not row:
                continue
            name, size = row
            size = int(size)
            file2size[name] = size

    return total_file_size, input_size, file2size


def setup_sites(config, total_size):
    # add a little more storage to sites for duplicated files
    final_size = 1.5 * total_size

    sites = config["sites"]

    small_count = 0
    large_count = 0

    for site in sites:
        if site['type'] == 'small':
            small_count += 1
        elif site['type'] == 'large':
            large_count += 1

    fractions = final_size / (small_count + (large_count * 2))

    id2site = dict()

    for site in sites:
        if site['type'] == 'small':
            site['storage'] = fractions
            id2site[site['id']] = fractions
        elif site['type'] == 'large':
            site['storage'] = fractions * 2
            id2site[site['id']] = fractions * 2

    return id2site


def distribute_input_files(config, id2storage, files2size):
    free_storage = copy.copy(id2storage)

    config_file_additions = list()

    # iterate input files
    for file, size in files2size.items():
        # find sites where the file would fit
        ids = set()
        for idd, available_storage in free_storage.items():
            if available_storage > size:
                ids.add(idd)
        target_id = random.choice(list(ids))
        free_storage[target_id] -= size

        config_file_additions.append(
            {
                "name": file,
                "siteId": target_id,
                "size": size
            }
        )
    config["files"] = config_file_additions


def random_trial(config, wf, run_counter, distribution_number, input_size):

    for j in range(RND_REPS):

        rnd_config = copy.deepcopy(config)

        name = f"RND_{run_counter}_{j}_{wf.get('name')}_{site_config.get('name')}_distribution{distribution_number}_n{N}_t{T}"

        print(f"[TRIAL {run_counter:05d} RND {j:02d}]: {name}")

        run_config_path = f'./evaluation_run_configs/{name}.json'

        # write run config
        with open(run_config_path, 'w') as jsonfile:
            json.dump(rnd_config, jsonfile, indent=4)

        # execute trial
        csv_path = f'{os.getcwd()}/evaluation_inputs/{wf["name"]}.csv'
        log_path = os.getcwd() + "/evaluation_logs/" + name + ".log"
        result_path = os.getcwd() + "/evaluation_results/" + name + ".csv"
        result = subprocess.run(
            ['java', '-cp', java_cp, 'federatedSim.FederatedTwoSites' , run_config_path, log_path, result_path ],
            capture_output=True,
            text=True,
            env=env
        )

        # check if a result was written, else it's a failure (which is acceptable under some circumstances)
        if not os.path.exists(result_path):
            with open(log_path, mode='a') as log_file:
                log_file.write("ERROR: \n")
                log_file.write(result.stderr)

            final_result_writer.writerow([wf.get('name'), 0, 0, 0, N, T, "FAILURE", rnd_config['name'], distribution_number, run_counter, j])
            continue

        # regular trial behaviour
        transferred_size = -1
        transferred_time = -1
        makespan = -1

        # read results
        with open(result_path, mode='r') as file:
            reader = csv.reader(file)
            for row in reader:
                if not row:
                    continue
                measure, value = row
                if measure == "TransferredData":
                    transferred_size = float(value)
                elif measure == "TransferTime":
                    transferred_time = float(value)
                elif measure == "Makespan":
                    makespan = float(value)


        transfer_factor = transferred_size / input_size
        transfer_factor_time = transferred_time / makespan

        final_result_writer.writerow([wf.get('name'), transfer_factor, transfer_factor_time, makespan, N, T, "SUCCESS", site_config['name'], i, run_counter, j])


site_conf_2_site = {
    'name': 'site_conf_2_site',
    'numSites': 2,
    'sites' : [
        {
            "id"   : 0,
            "type" : 'small',
            "ram"  : 2048,
            "mips" : 100,
            "pes"  : 4,
            "intraBw" : 1.2e7
        },
        {
            "id"   : 1,
            "type" : 'large',
            "ram"  : 10 * 2048,
            "mips" : 100,
            "pes"  : 10 * 4,
            "intraBw" : 2 * 1.2e7
        }

    ],
    'connections' : [
        { "site1Id": 0, "site2Id": 1, "bandwidth": 1.2e7 }
    ]
}


site_conf_3_site_small = copy.deepcopy(site_conf_2_site)
site_conf_3_site_small['name'] = 'site_conf_3_site_small'
site_conf_3_site_small['numSites'] = 3
site_conf_3_site_small['sites'].append(
    {
        "id"   : 2,
        "type" : 'small',
        "ram"  : 2048,
        "mips" : 100,
        "pes"  : 4,
        "intraBw" : 1.2e7
    }
)
site_conf_3_site_small['connections'].extend([
    {"site1Id": 0, "site2Id": 2, "bandwidth": 6e6},
    {"site1Id": 1, "site2Id": 2, "bandwidth": 6e6}
])


site_conf_3_site_large = copy.deepcopy(site_conf_2_site)
site_conf_3_site_large['name'] = 'site_conf_3_site_large'
site_conf_3_site_large['numSites'] = 3
site_conf_3_site_large['sites'].append(
    {
        "id"   : 2,
        "type" : 'large',
        "ram"  : 10 * 2048,
        "mips" : 100,
        "pes"  : 10 * 4,
        "intraBw" : 2 * 1.2e7
    }
)
site_conf_3_site_large['connections'].extend([
    {"site1Id": 0, "site2Id": 2, "bandwidth": 6e6},
    {"site1Id": 1, "site2Id": 2, "bandwidth": 6e6}
])

site_conf_4_site = copy.deepcopy(site_conf_2_site)
site_conf_4_site['name'] = 'site_conf_4_site'
site_conf_4_site['numSites'] = 4
site_conf_4_site['sites'].extend([
    {
        "id"   : 2,
        "type" : 'large',
        "ram"  : 10 * 2048,
        "mips" : 100,
        "pes"  : 10 * 4,
        "intraBw" : 2 * 1.2e7
    },
    {
        "id"   : 3,
        "type" : 'small',
        "ram"  : 2048,
        "mips" : 100,
        "pes"  : 4,
        "intraBw" : 1.2e7
    }
])

site_conf_4_site['connections'].extend([
    {"site1Id": 2, "site2Id": 0, "bandwidth": 6e6},
    {"site1Id": 2, "site2Id": 1, "bandwidth": 6e6},
    {"site1Id": 2, "site2Id": 3, "bandwidth": 6e6},
    {"site1Id": 3, "site2Id": 0, "bandwidth": 6e6},
    {"site1Id": 3, "site2Id": 1, "bandwidth": 6e6},
])

site_configs = [site_conf_2_site, site_conf_3_site_small, site_conf_3_site_large, site_conf_4_site]

java_cp = '../bin'

if __name__ == "__main__":

    sys.path.append(os.path.join(os.getcwd(), 'synthetic_wf_scripts'))

    import distribution
    import aggregation
    import groups
    import longPipeline
    import multiPipeline
    import redistribution

    # create dirs
    os.makedirs('evaluation_inputs', exist_ok=True)
    os.makedirs('evaluation_run_configs', exist_ok=True)
    os.makedirs('evaluation_logs', exist_ok=True)
    os.makedirs('evaluation_results', exist_ok=True)
    os.makedirs('evaluation_synthetic_wfs', exist_ok=True)


    wfs = generate_synthetic_workflows()
    wfs.extend(get_real_workflows())

    # set java env
    env = os.environ.copy()
    java_home = "/usr/lib/jvm/jdk1.8.0_202"
    env['JAVA_HOME'] = java_home
    env['PATH']      = f"{java_home}/bin:" + env['PATH']
    java_cp = "../lib/*:../bin:../../workflowSim_git/lib/*"

    # final result path
    final_result_file = os.getcwd() + "/evaluation_final_results.csv"
    with open(final_result_file, mode='w', newline='') as result_file:
        final_result_writer = csv.writer(result_file)
        final_result_writer.writerow(['NAME', 'TRANSFER-FACTOR-SIZE', 'TRANSFER-FACTOR-TIME', 'MAKESPAN', 'N', 'T', 'STATUS', 'CONFIG', 'DISTRIBUTION', 'RUN_COUNT', 'RND_COUNT'])

        run_counter = 0
        for wf in wfs:

            total_size, input_size, file_map = get_files_and_size(wf)

            for site_config in site_configs:

                base_config = copy.deepcopy(site_config)
                base_config["workflowPath"] = wf["path"]
                id2storage = setup_sites(base_config, total_size)

                # distribute input files
                for i in range(NUM_DISTRIBUTIONS):

                    distribution_config = copy.deepcopy(base_config)
                    distribute_input_files(distribution_config, id2storage, file_map)

                    run_counter+=1
                    # iterate approaches
                    for approach in ('PART', 'RND'):

                        run_config = copy.deepcopy(distribution_config)

                        run_config['strategy'] = approach

                        run_config['taskThreshold'] = N
                        run_config['secThreshold']  = T

                        if approach == 'RND':
                            random_trial(run_config, wf, run_counter, i, input_size)
                            continue

                        name = f"{approach}_{run_counter}_{wf.get('name')}_{site_config.get('name')}_distribution{i}_n{N}_t{T}"

                        print(f"[TRIAL {run_counter:05d}]: {name}")

                        run_config_path = f'./evaluation_run_configs/{name}.json'

                        # write run config
                        with open(run_config_path, 'w') as jsonfile:
                            json.dump(run_config, jsonfile, indent=4)

                        # execute trial
                        csv_path = f'{os.getcwd()}/evaluation_inputs/{wf["name"]}.csv'

                        log_path = os.getcwd() + "/evaluation_logs/" + name + ".log"
                        result_path = os.getcwd() + "/evaluation_results/" + name + ".csv"

                        result = subprocess.run(
                            ['java', '-cp', java_cp, 'federatedSim.FederatedTwoSites' , run_config_path, log_path, result_path ],
                            capture_output=True,
                            text=True,
                            env=env
                        )

                        # check if a result was written, else it's a failure (which is acceptable under some circumstances)
                        if not os.path.exists(result_path):
                            with open(log_path, mode='a') as log_file:
                                log_file.write("ERROR: \n")
                                log_file.write(result.stderr)

                            final_result_writer.writerow([wf.get('name'), 0, 0, 0, N, T, "FAILURE", site_config['name'], i, run_counter, -1])
                            continue

                        # regular trial behaviour
                        transferred_size = -1
                        transferred_time = -1
                        makespan = -1

                        # read results
                        with open(result_path, mode='r') as file:
                            reader = csv.reader(file)
                            for row in reader:
                                if not row:
                                    continue
                                measure, value = row
                                if measure == "TransferredData":
                                    transferred_size = float(value)
                                elif measure == "TransferTime":
                                    transferred_time = float(value)
                                elif measure == "Makespan":
                                    makespan = float(value)


                        transfer_factor = transferred_size / input_size
                        transfer_factor_time = transferred_time / makespan

                        final_result_writer.writerow([wf.get('name'), transfer_factor, transfer_factor_time, makespan, N, T, "SUCCESS", site_config['name'], i, run_counter, -1])


