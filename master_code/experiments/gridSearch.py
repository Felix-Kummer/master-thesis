import copy
import os
import random
import sys
import subprocess
import csv
import json

SYNTHETIC_WFS = 5
NUM_DISTRIBUTIONS = 5


def generate_synthetic_workflows():
    wfs = list()

    for i in range(SYNTHETIC_WFS):
        wf = dict()
        wf['name'] = f"aggregation{i}"
        path = f"{os.getcwd()}/grid_search_synthetic_wfs/{wf['name']}.xml"
        wf['path'] = path
        aggregation.run(str(path))
        wfs.append(wf)

        wf = dict()
        wf['name'] = f"distribution{i}"
        path = f"{os.getcwd()}/grid_search_synthetic_wfs/{wf['name']}.xml"
        wf['path'] = path
        distribution.run(str(path))
        wfs.append(wf)

        wf = dict()
        wf['name'] = f"groups{i}"
        path = f"{os.getcwd()}/grid_search_synthetic_wfs/{wf['name']}.xml"
        wf['path'] = path
        groups.run(str(path))
        wfs.append(wf)

        wf = dict()
        wf['name'] = f"longPipeline{i}"
        path = f"{os.getcwd()}/grid_search_synthetic_wfs/{wf['name']}.xml"
        wf['path'] = path
        longPipeline.run(str(path))
        wfs.append(wf)

        wf = dict()
        wf['name'] = f"multiPipeline{i}"
        path = f"{os.getcwd()}/grid_search_synthetic_wfs/{wf['name']}.xml"
        wf['path'] = path
        multiPipeline.run(str(path))
        wfs.append(wf)

        wf = dict()
        wf['name'] = f"redistribution{i}"
        path = f"{os.getcwd()}/grid_search_synthetic_wfs/{wf['name']}.xml"
        wf['path'] = path
        redistribution.run(str(path))
        wfs.append(wf)

    return wfs

def get_real_workflows():
    wfs = list()

    wf = dict()
    wf['name'] = "CyberShake_30"
    wf['path'] = '../../config/dax/CyberShake_30.xml'
    wfs.append(wf)

    wf = dict()
    wf['name'] = "CyberShake_50"
    wf['path'] = '../../config/dax/CyberShake_50.xml'
    wfs.append(wf)

    wf = dict()
    wf['name'] = "CyberShake_100"
    wf['path'] = '../../config/dax/CyberShake_100.xml'
    wfs.append(wf)

    wf = dict()
    wf['name'] = "CyberShake_1000"
    wf['path'] = '../../config/dax/CyberShake_1000.xml'
    wfs.append(wf)

    wf = dict()
    wf['name'] = "Epigenomics_24"
    wf['path'] = '../../config/dax/Epigenomics_24.xml'
    wfs.append(wf)

    wf = dict()
    wf['name'] = "floodplain"
    wf['path'] = '../../config/dax/floodplain.xml'
    wfs.append(wf)

    wf = dict()
    wf['name'] = "Sipht_30.xml"
    wf['path'] = '../../config/dax/Sipht_30.xml'
    wfs.append(wf)

    return wfs


def get_files_and_size(wf):
    # get input files and sizes
    csv_path = f'{os.getcwd()}/grid_search_inputs/{wf["name"]}.csv'
    result = subprocess.run(
        ['java', '-cp', java_cp, 'federatedSim.getInputFilesHelper' , f'{wf["path"]}', csv_path],
        capture_output=True,
        text=True,
        env=env
    )

    # get input files and their sizes, and the total size of the workflow
    total_size = 0
    file2size = dict()
    with open(csv_path, mode='r') as file:
        reader = csv.reader(file)
        total_size = int(next(reader)[1])  # first line contains total size
        input_size = int(next(reader)[1])  # second line contains total input size

        for row in reader:
            if not row:
                continue
            name, size = row
            size = int(size)
            file2size[name] = size

    return total_size, input_size, file2size


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


site_conf_2_site = {
    'name': 'site_conf_2_site',
    'numSites': 2,
    'strategy' : 'PART',
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
    os.makedirs('grid_search_inputs', exist_ok=True)
    os.makedirs('grid_search_run_configs', exist_ok=True)
    os.makedirs('grid_search_logs', exist_ok=True)
    os.makedirs('grid_search_results', exist_ok=True)
    os.makedirs('grid_search_synthetic_wfs', exist_ok=True)


    wfs = generate_synthetic_workflows()
    wfs.extend(get_real_workflows())

    # set java env
    env = os.environ.copy()
    java_home = "/usr/lib/jvm/jdk1.8.0_202"
    env['JAVA_HOME'] = java_home
    env['PATH']      = f"{java_home}/bin:" + env['PATH']
    java_cp = "../lib/*:../bin:../../lib/*"

    # final result path
    final_result_file = os.getcwd() + "/grid_search_final_results.csv"
    with open(final_result_file, mode='w', newline='') as result_file:
        final_result_writer = csv.writer(result_file)
        final_result_writer.writerow(['NAME', 'TRANSFER-FACTOR', 'N', 'T', 'STATUS', 'CONFIG', 'DISTRIBUTION'])

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

                    # hyperparams
                    for n in [0, 1, 10, 100]:
                        for t in [0, 1, 10, 60]:
                            run_counter += 1

                            name = f"{wf.get('name')}_{site_config.get('name')}_distribution{i}_n{n}_t{t}"

                            print(f"[TRIAL {run_counter:05d}]: {name}")

                            run_config = copy.deepcopy(distribution_config)

                            run_config['taskThreshold'] = n
                            run_config['secThreshold']  = t

                            run_config_path = f'./grid_search_run_configs/{name}.json'

                            # write run config
                            with open(run_config_path, 'w') as jsonfile:
                                json.dump(run_config, jsonfile, indent=4)

                            # execute trial
                            csv_path = f'{os.getcwd()}/grid_search_inputs/{wf["name"]}.csv'

                            log_path = os.getcwd() + "/grid_search_logs/" + name + ".log"
                            result_path = os.getcwd() + "/grid_search_results/" + name + ".csv"

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

                                final_result_writer.writerow([wf.get('name'), 0, n, t, "FAILURE", site_config['name'], i])
                                continue

                            # regular trail behaviour
                            transferred_size = 0

                            # read results
                            with open(result_path, mode='r') as file:
                                reader = csv.reader(file)
                                for row in reader:
                                    if not row:
                                        continue
                                    measure, value = row
                                    if measure == ("TransferredData"):
                                        transferred_size = float(value)

                            transfer_factor = transferred_size / input_size

                            final_result_writer.writerow([wf.get('name'), transfer_factor, n, t, "SUCCESS", site_config['name'], i])


