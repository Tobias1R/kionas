import toml
import json
import os

CONFIGS_DIR = '/workspace/configs'

def parse_env_vars(value):
    if isinstance(value, str) and '${' in value and '}' in value:
        start = value.find('${') + 2
        end = value.find('}', start)
        env_var = os.getenv(value[start:end])
        new_value = env_var + value[end+1:]
        print(f"Parsed env var: {value} -> {new_value}")
        return new_value
    return value

for fname in os.listdir(CONFIGS_DIR):
    if fname.startswith('worker') and fname.endswith('.toml'):
        worker_name = fname.replace('.toml', '')
        toml_path = os.path.join(CONFIGS_DIR, fname)
        with open(toml_path, 'r') as f:
            worker_conf = toml.load(f)
        # Parse environment variables
        for key, value in worker_conf.items():
            worker_conf[key] = parse_env_vars(value)
        json_path = os.path.join(CONFIGS_DIR, f'{worker_name}.json')
        with open(json_path, 'w') as f:
            json.dump(worker_conf, f, indent=2)
        print(f'Generated {json_path}')
