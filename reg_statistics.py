# -*- coding: utf-8 -*-

import pandas as pd
import argparse
import sys
import os
import monitor_functions as mf

parser = argparse.ArgumentParser()
parser.add_argument('-f', help='folder for report', dest='folder',
                    type=str)
parser.add_argument('-i', help='getting name of csv file', dest='input_file',
                    type=str)

args = parser.parse_args()

if args.input_file[-4:] == '.csv':
    df = pd.read_csv('data/csv/{}'.format(args.input_file))
else:
    df = pd.read_csv('data/csv/{}.csv'.format(args.input_file))

regs = os.listdir('data/dicts/regs_sc/')
regs = [each[:-5] for each in regs]

regions_names_dict = pd.read_excel('data/dicts/region_codes.xlsx').set_index(
        'region_code').to_dict()['region_name']

full_sc_list = []
os.makedirs('reports/reg/NW_region/{0}'.format(args.folder))

for idx, region in enumerate(regs):
    sys.stdout.write("\rAdding region to service centers list \n")
    sys.stdout.write("\rBanks progress: {0}> \n".format("="*idx))
    full_sc_list += [str(each) for each in pd.read_excel(
            'data/dicts/regs_sc/{}.xlsx'.format(region))['ID'].tolist()]
    sys.stdout.write("\rRegion in progress: {} \n".format(
            regions_names_dict[int(region)]))
    reg_df = pd.read_excel('data/dicts/regs_sc/{}.xlsx'.format(region))

    raId_dict = reg_df['ID'].tolist()
    raId_dict = [str(each) for each in raId_dict]

    try:
        os.makedirs('reports/reg/{0}/{1}'.format(
                regions_names_dict[int(region)], args.folder))
    except FileExistsError:
        # directory already exists
        raise FileExistsError

    r_df = df.loc[df['service_centre'].isin(raId_dict)]
    mf.output_tables(r_df, args, folder='reg/{0}/{1}'.format(
            regions_names_dict[int(region)], args.folder))
    sys.stdout.flush()

# this is too slow need O(*) improovement
nw_df = df.loc[df['service_centre'].isin(full_sc_list)]
mf.output_tables(nw_df, args, folder='reg/NW_region/{0}'.format(args.folder))
