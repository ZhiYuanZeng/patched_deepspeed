import torch
from petrel_client.client import Client
import io
from urllib.parse import urlparse
import fnmatch
import os
import glob
from copy import deepcopy
from functools import partial

def torch_save_maybe_aws(model_obj, file_obj, conf_path = '~/petreloss.conf', torch_save_func=None, **kargs):
    print('the torch.save function is hacked to support aws file path, if the file_path starswith {s3://}, it will be saved with s3 sdk, otherwise the default torch.save is used')
    if isinstance(file_obj, str) and file_obj.startswith('s3://'):
        client = Client(conf_path)
        with io.BytesIO() as f:
            torch.save(model_obj, f, **kargs)
            client.put(file_obj, f.getvalue())
    else:
        if torch_save_func is not None:
            torch_save_func(model_obj, file_obj, **kargs)
        else:
            torch.save(model_obj, file_obj, **kargs)

def torch_load_maybe_aws(file_obj, map_location=None, conf_path = '~/petreloss.conf', torch_load_func=None, **kargs):
    print('the torch.load function is hacked to support aws file path, if the file_path starswith {s3://}, it will be saved with s3 sdk, otherwise the default torch.load is used')
    if isinstance(file_obj, str) and file_obj.startswith('s3://'):
        client = Client(conf_path)
        download_buffer = client.get(file_obj)
        seekable_buffer = io.BytesIO(download_buffer)
        model_obj = torch.load(seekable_buffer, map_location, **kargs)
    else:
        if torch_load_func is not None:
            model_obj = torch_load_func(file_obj, map_location=map_location, **kargs)
        else:
            model_obj = torch.load(file_obj, map_location=map_location, **kargs)

    return model_obj

def glob_maybe_aws(path, glob_func=None):
    def extract_s3_pattern(path):
        parsed_url = urlparse(path)
        prefix = f"{parsed_url.scheme}://{parsed_url.netloc}{os.path.dirname(parsed_url.path)}/"
        pattern = path.split('/')[-1]
        return prefix, pattern
    
    def filter_strings(strings, expression):
        matched_strings = [string for string in strings if fnmatch.fnmatchcase(string, expression)]
        return matched_strings

    if path.startswith('s3://'):
        # Parse the pattern to extract the bucket name and key
        prefix, pattern = extract_s3_pattern(path)

        # Create a boto3 S3 client
        client = Client('~/petreloss.conf')
        files = client.get_file_iterator(prefix)
        files = [f[0].split('/')[-1] for f in files]
        matched_strings = filter_strings(files, pattern)
        matched_strings = [prefix+f for f in matched_strings]
        return matched_strings
    else:
        if glob_func is not None:
            return glob_func(path)
        else:
            return glob(path)

def hack_torch_save():
    torch_save_func = deepcopy(torch.save)
    torch.save = partial(torch_save_maybe_aws, torch_save_func=torch_save_func)

def hack_torch_load():
    torch_load_func = deepcopy(torch.load)
    torch.load = partial(torch_load_maybe_aws, torch_load_func=torch_load_func)

def hack_glob():
    glob_func = deepcopy(glob.glob)
    glob.glob = partial(glob_maybe_aws, glob_func=glob_func)

def hack_torch_io_to_support_aws():
    hack_torch_save()
    hack_torch_load()
    hack_glob()

if __name__=='__main__':
    hack_torch_io_to_support_aws()

    print(glob.glob('s3://zengzhiyuan.d/*'))
    print(glob.glob('/mnt/petrelfs/zengzhiyuan.d/mollm/output/stanford_alpaca_7B/step_10/pytorch_model/dp_rank_0_mp_rank_*_model_states.pt'))
    model = torch.nn.Linear(10,10)
    torch.save(model, 's3://zengzhiyuan.d/dummy_ckpt/ckpt.bin')
    loaded_model = torch.load('s3://zengzhiyuan.d/dummy_ckpt/ckpt.bin')
    print(loaded_model)

    torch.save(model, './ckpt.bin')
    loaded_model = torch.load('./ckpt.bin')
    print(loaded_model)
