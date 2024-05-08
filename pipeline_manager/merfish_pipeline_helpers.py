import json
import configparser

CONFIG_PATH = "config.master.ini"

# This is a short term expansion of the MerfishAnalysis class. Talk to Colin about properly implementing it in MERFISH tools fileio.py
class MerscopeDirectory:
    pass

class MerfishExpiriment:
    pass

# TODO: move to db manager
def find_new_exp() -> list:
    # TODO: access config for json file prefix
    with open("expiriment_log.json", "r") as json_file:
        full_log = json.load(json_file)

    new_merscope_dir_dict = {}
    for ms_path, ms_dir_obj in full_log.items():
        MscopeDir = MerscopeDirectory.from_json(ms_dir_obj)
        MscopeDir._initialize_expiriments(done=False)
        new_merscope_dir_dict[ms_path] = MscopeDir.to_dict()

    new_expiriments_list = []
    for ms_path, ms_dir_obj in new_merscope_dir_dict.items():
        inc_ex = ms_dir_obj["incomplete_exp"]
        if inc_ex:
            new_expiriments_list += [MerfishExpiriment.from_json(n_ex) for n_ex in inc_ex.values()]

    return new_expiriments_list, new_merscope_dir_dict

# TODO: figure out what to do with this
def read_expiriment_log():    
    with open("expiriment_log.json", "r") as json_file:
        full_log = json.load(json_file)

    merscope_dir_dict = {}
    for ms_path, ms_dir_obj in full_log.items():
        MscopeDir = MerscopeDirectory.from_json(ms_dir_obj)
        merscope_dir_dict[ms_path] = MscopeDir

    return merscope_dir_dict

# TODO: this can stay
def load_config(config_file:str=CONFIG_PATH) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    with open(config_file, "r") as conf_file_conn:
        config.read_file(conf_file_conn)
        if not config.sections():
            raise RuntimeError("passed config file connection has failed or does not have any sections")
        return config

# TODO: move to scheduler; probably rewrite
def assemble_directory_paths(expiriment:MerfishExpiriment) -> dict:

    conf = load_config()
    io_conf = conf["IO Options"]
    
    merscope_dir_obj = read_expiriment_log()[expiriment.ms_path] 

    # pull input
    MERSCOPE_DIR = merscope_dir_obj.root
    MER_RAWDATA_DIR = merscope_dir_obj.subdir['data']
    MER_OUTPUT_DIR = merscope_dir_obj.subdir['output']
    MER_BARCODES = io_conf['barcodes']

    # pull output
    MER_POSTPROC=io_conf["mer_postprocess"]
    CELLPOSE_DIR=io_conf["cellpose"]
    CELL_BY_GENE_TAB = io_conf['cell_by_gene_tab']
    MASKS_DIR=io_conf["masks"]

    # Get current directory
    CWD = os.getcwd()
    
    ### Build input and output paths from variables in Config file and expiriment info
    paths = {}

    ### Input ###
    paths["merscope"] = f"{MERSCOPE_DIR}"
    paths["raw_data_dir"] = f"{paths['merscope']}/{MER_RAWDATA_DIR}/{expiriment.name}" # Used to load imageset
    paths["output_dir"] = f"{paths['merscope']}/{MER_OUTPUT_DIR}/{expiriment.name}" # Used to find barcodes
    paths['barcodes'] = f"{paths['output_dir']}/{MER_BARCODES}"

    ### Output ### 
    paths["postprocess_dir"] = f"{CWD}/{MER_POSTPROC}/{expiriment.name}" # output of snakefile to avoid messing with the original data
    paths["done_flag"] = f"{paths["postprocess_dir"]}/done.fla"
    paths["config"] = f"{paths["postprocess_dir"]}/config.ini" # copy of config file for later pipeline fiddling
    # Cellpose
    paths["cellpose_dir"] = f"{paths['postprocess_dir']}/{CELLPOSE_DIR}" # used to save final cellpose output
    paths["cell_by_gene_tab"] = f"{paths['cellpose_dir']}/{CELL_BY_GENE_TAB}" # used to save final cellpose output
    paths["masks_dir"] = f"{paths['cellpose_dir']}/{MASKS_DIR}" # used to save masks

    return paths


if __name__ == "__main__":
    # test_md = MerscopeDirectory("/mnt/merfish15/MERSCOPE")
    # test_md_dict = test_md.to_dict()
    # print(test_md_dict)
    # print(test_md_dict["incomplete_exp"])
    # print(test_md_dict["complete_exp"])
    # initialize_merscope_dirs()
    # print(find_new_exp())
    test = find_new_exp()
    print(test[0])
    print(test[1])