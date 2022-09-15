from dataclasses import dataclass


@dataclass
class ModuleImport:
    '''Class for storing Imports.'''
    module: list
    name: list
    alias: str = None


def parse_import(node):
    module = []

    for n in node.names:
        return ModuleImport(module, n.name.split('.'), n.asname)


def parse_importfrom(node):
    module = node.module.split('.')

    for n in node.names:
        return ModuleImport(module, n.name.split('.'), n.asname)
