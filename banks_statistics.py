# -*- coding: utf-8 -*-

import pandas as pd
import argparse
import os
import shutil
import monitor_functions as mf
import waves

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

selectors = {
        'first_wave': waves.first_wave_banks,
        'second_wave': waves.sec_wave_banks,
        'upd_first_wave': waves.updated_banks,
        'not_upd_first_wave': waves.not_updated_banks,
        }

try:
    os.makedirs('reports/{}'.format(args.folder))
    for key, value in selectors.items():
        os.makedirs('reports/{}/{}_{}'.format(args.folder, args.folder, key))
except FileExistsError:
    # directory already exists
    folder = 'reports/{}'.format(args.folder)
    print('Ууупс, похоже ваш отчет уже существует! Вы уверены, что хотите удалить его и создать новый?\n[y/n]')
    y_n = input()
    if y_n.lower() == 'y' or y_n.lower() == 'да':
        shutil.rmtree(folder)
        os.makedirs('reports/{}'.format(args.folder))
        for key, value in selectors.items():
            os.makedirs('reports/{}/{}_{}'.format(
                    args.folder, args.folder, key))
    else:
        raise FileExistsError('Новый отчет не создан. Старый не тронут.')

mf.output_tables(df, args)

for idx, (key, value) in enumerate(selectors.items()):
    _df = df.loc[df['bank'].isin(value)]
    mf.output_tables(_df, args, folder='{}/{}_{}'.format(args.folder,
                     args.folder,
                     key))
