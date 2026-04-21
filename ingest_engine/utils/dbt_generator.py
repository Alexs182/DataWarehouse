import sys
sys.path.insert(0, '..')

import os
import yaml
import argparse
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

class TemplateWriter:
    def __init__(self, template_root):
        self.template_root = template_root
        self.template_folder = os.path.join(ROOT_DIR, self.template_root)
        self.enviornment = Environment(loader=FileSystemLoader(self.template_folder))

        self.params = {}

    def generate_template(self, template_name):
        template = self.enviornment.get_template(f'{template_name}.sql')
        content = template.render(params=self.params)

        return content

    def write_content_to_file(self, filename, content):
        self.create_folder_path(filename)

        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)

    def create_folder_path(self, filename):
        """ build the dir if it doesn't all ready exist """
        filename_array = filename.split('/')
        folder_name = "/".join(filename_array[:len(filename_array)-1])
        Path(folder_name).mkdir(parents=True, exist_ok=True)

    def run(self, config):
        pipeline_name = config.get('pipeline_name', "")
        template_prefix = config.get("template_prefix", "")

        for template in config.get('templates', []):
            content = self.generate_template(
                template
            )

            filename = f'{self.template_folder}/generated/{pipeline_name}/{template_prefix}_{template}.sql'
            self.write_content_to_file(filename, content)



def main(args):
    config = get_config(args.config)

    TemplateWriter(
        template_root="templates/"
    ).run(
        config=config.get('sql', {})
    )

def get_config(config_file: str):
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

            return config
    except FileNotFoundError:
        raise Exception("Missing config for execution")

def add_args():
    parser = argparse.ArgumentParser(
        description="SQL generator"
    )

    parser.add_argument("-c", "--config", type=str, required=True, help="Path to config file")

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
    args = add_args()
    main(args)