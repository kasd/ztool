#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# author: a.kulpinov@gmail.com

import os
import click
import kazoo
from kazoo.client import KazooClient


@click.group()
@click.option('--verbose', is_flag=True, help='Enables verbose mode.')
@click.pass_context
def cli(ctx, verbose: bool):
    """Simple zookeeper import/export tool"""
    ctx.obj = {'verbose': verbose}
    pass

@cli.command("export")
@click.option('--zpath', help='Zookeeper path', default='/')
@click.option('--zaddress', help='Zookeeper address', default='localhost:2181')
@click.option('--dest_dir', help='Destination directory', default='zdata')
@click.option('--zdata', help='Zookeeper data file', default='___zdata___')
@click.pass_context
def export_impl(ctx: click.core.Context, 
           zpath: str, 
           zaddress: str, 
           dest_dir: str, zdata: str) -> None:
    """Export zookeeper data to directory"""
    
    zk = KazooClient(hosts=zaddress)
    zk.start()

    stack = [zpath]
    try:
        while len(stack) > 0:
            path = stack.pop()
            children = zk.get_children(path)
            for child in children:
                stack.append(f"{path}/{child}")

            if ctx.obj['verbose']:
                print(f"Dumping {path}")

            data, stat = zk.get(path)

            if not os.path.exists(f"{dest_dir}{path}"):
                os.makedirs(f"{dest_dir}{path}")

            if stat.dataLength > 0:
                with open(f"{dest_dir}{path}/{zdata}", "wb") as f:
                    f.write(data)
        
        print(f"ZooKeeper data exported to {dest_dir}")

    finally:
        zk.stop()


@cli.command("import")
@click.option('--zpath', help='Zookeeper path', default='/')
@click.option('--zaddress', help='Zookeeper address', default='localhost:2181')
@click.option('--src_dir', help='Source directory', default='zdata')
@click.option('--zdata', help='Zookeeper data file', default='___zdata___')
@click.pass_context
def import_impl(ctx: click.core.Context, zpath: str, zaddress: str, src_dir: str, zdata: str) -> None:
    """Import zookeeper data from directory"""

    def find_zdata_files(root_dir):
        """Find all zdata files in directory and subdirectories"""
        stack = [root_dir]

        while len(stack) > 0:
            current_dir = stack.pop()
            try:
                with os.scandir(current_dir) as entries:
                    for entry in entries:
                        if entry.is_dir():
                            stack.append(entry.path)
                        elif entry.is_file() and entry.name == zdata:
                            yield entry.path
            except OSError as e:
                print(f"Error while scanning {current_dir}: {e}")

    zk = KazooClient(hosts=zaddress)
    zk.start()

    try:
        for zdata_file in find_zdata_files(src_dir):
            znode = zdata_file[:-len(zdata) - 1]

            if ctx.obj['verbose']:
                print(f"Importing {znode}")

            data = open(zdata_file, "rb").read()
            try:
                zk.set(f"{zpath}/{znode}", data)
            except kazoo.exceptions.NoNodeError:
                zk.create(f"{zpath}/{znode}", data)
    finally:
        zk.stop()

if __name__ == '__main__':
    cli()