import sys
sys.path.insert(0, '..')

import os
import yaml
import argparse

from jinja2 import Environment, FileSystemLoader

class TemplateWriter:
    def __init__(self, template_root):
        self.template_root = template_root
        self.template_folder = os.path.join(ROOT_DIR, self.template_root)
        self.enviornment = Environment(loader=FileSystemLoader(self.template_folder))

    def generate_template(self, template_name):
        template = self.enviornment.get_template(template_name)
        content = template.render(params=self.params)

        return content

    def write_content_to_file(self, filename, content):

        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)

    def run(self, config):

        for template in config.get('templates', []):
            content = self.generate_template(
                template
            )








def main(args):
    config = get_config(args.config)

    TemplateWriter(
        templates_folder="templates/"
    ).run(
        config=config.get('sql', {})
    )

def get_config(config_file: str, logger):
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

            return config
    except FileNotFoundError:
        logger.error(f"Unable to open file: {config_file}")
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