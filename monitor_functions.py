# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from datetime import datetime
import re
import sys

"""
Functions:
    - @emptyDfError: decorator that checks if DataFrame is empty;
    rais the exception if df.shape[0] is 0.

    - bank_df(df, bank): return df contains only selected bank;
    bank is str and df checked for emptiness

    - df_notna(df, field): return df cleared form nan's in selected fiels;
    field is str and df checked for emptiness

    - set_df_field(df, field): return list of unique vals from selected field;
    field is str and df checked for emptiness

    - get_df_by_condition(df, *args, used_args = 0): return df by selecting
    conditions stepwise;
    *args is tuples of (field, condition)

    - date_match(date_string, *args): checks if datetime in column in *args;
    date_string is string form df; args format is '%Y-%m-%d'

    - df_by_dates(df, *args): returns df for dates in *args;
    args format is '%Y-%m-%d'; df checked for emptiness

    - error_format(error): checks for int(error).
    if not, tries to make list of errors (like ['104', '108'])

    - set_column_names(df): Gives set of errors names;

    - get_code_from_val(code_dict, val): return key by dict value.

    - error_count(df, error, photo=None, sound=None, other=None):
    count number of given error in dataframe.

    -form_table(df, columns): return dict you can use to output to the file;
    The main table with dates and errors stuff.

    - total2df(df, columns): return dict you can use to output to the file;
    The secondary table of banks subspace with dates and errors stuff.

    - df2table(df, writer, bank): write excel file to the disk.
    Contains cell formats and writer.

    - output_tables(df, args, folder=None): wraper to call df2table(...).

    - rebank(bank): if name of bank is too long, shorten it
"""


photo_code = pd.read_excel('data/dicts/photo_codes.xlsx', header=None)
sound_code = pd.read_excel('data/dicts/sound_codes.xlsx', header=None)
other_code = pd.read_excel('data/dicts/other_codes.xlsx', header=None)
photo_code_dicts = dict(zip(photo_code[1], photo_code[0]))
sound_code_dicts = dict(zip(sound_code[1], sound_code[0]))
other_code_dicts = dict(zip(other_code[1], other_code[0]))


# Service functions
# I like useless things. Why not?
def emptyDfError(func):
    def wrapper(*args, **kwargs):
        if args[0].shape[0] == 0:
            raise Exception(
                    'Input DataFrame is empty. df.shape[0] == {}'.format(
                            args[0].shape[0]))
        result = func(*args, **kwargs)
        if len(result) == 0 or result.shape[0] == 0:
            raise Exception(
                    'Returned list is empty, because output DataFrame shape[0] == 0. Args is: {}. Kwargs is: {}'.format(args, kwargs))
        return result
    return wrapper


@emptyDfError
def bank_df(df, bank):
    return df.loc[df['bank'] == bank]


# @emptyDfError
# field is str data type
def df_notna(df, field):
    return df.loc[df[field].notna()]


def set_df_field(df, field):
    set_df = df_notna(df, field)
    return list(set(set_df[field].tolist()))


# should return DataFrame
@emptyDfError
def get_df_by_condition(df, *args, used_args=0):
    if used_args == len(args):
        return df
    elif len(args) >= 1:
        while used_args < len(args):
            return get_df_by_condition(
                    df.loc[df[args[used_args][0]] == args[used_args][1]],
                    *args,
                    used_args=used_args+1)


def date_match(date_string, *args):
    dt = datetime.strptime(date_string, '%Y.%m.%d %H:%M:%S')
    # datetime matching list - dtml
    dtml = [datetime.strptime(arg, '%Y-%m-%d').date() for arg in args]
    if dt.date() in dtml:
        return True
    else:
        return False


@emptyDfError
def df_by_dates(df, *args):
    mask = [date_match(each, *args) for each in df['init_date_time']]
    return df[mask]


# need to add statuses/errors like '200/413'
def error_format(error):
    try:
        int(error)
        return([error])
    except ValueError:
        if isinstance(error, str):
            tmp_error = re.split(r'[^0-9]', error)
            string = list(filter(None, tmp_error))
        else:
            raise TypeError('Errors are not strings! Check it out please')
        return string


def set_column_names(df):
    column_names = ['Дата',
                    'Всего попыток регистрации',
                    'Успешные регистрации',
                    '% успешных',
                    'Всего неуспешных',
                    '% неуспешных']

    codes = ['bkk_photo_status_code', 'bkk_sound_status_code']

    photo_codes = set_df_field(df, codes[0])
    perrors = [error_format(each) for each in photo_codes]
    perrors = [each for sublist in perrors for each in sublist if each != '0']
    perrors_name = [photo_code_dicts[int(error)] for error in set(perrors)]

    sound_codes = set_df_field(df, codes[1])
    serrors_name = [sound_code_dicts[int(error)] for error in set(sound_codes)]

    filtered_df = df_notna(df, 'success')
    _filtered_df = filtered_df.loc[(filtered_df['success'] != 200) &
                                   (filtered_df['success'] != 202) &
                                   (filtered_df['success'] != 414)]
    other_codes = set_df_field(_filtered_df, 'success')
    oerrors_name = [other_code_dicts[int(error)] for error in set(other_codes)]
    _filtered_df = filtered_df.loc[(filtered_df['success'] == 414)]
    error414 = list(set(['{0}. Модальность: {1}'.format(
                other_code_dicts[414], error.split('[')[1])
                for error in _filtered_df['smev_exit_code_description']]))
    oerrors_name = oerrors_name + error414
    _filtered_df = df.loc[(df['success'].isna()) &
                          (df[codes[0]].isna()) &
                          (df[codes[1]].isna())]
    lost_codes = set_df_field(_filtered_df, 'smev_exit_code_description')
    oerrors_name = oerrors_name + lost_codes

    return (column_names, perrors_name, serrors_name, oerrors_name)


def get_code_from_val(code_dict, val):
    try:
        # separates the dictionary's values in a list, finds the position of
        # the value you have, and gets the key at that position
        # https://stackoverflow.com/questions/8023306/get-key-by-value-in-dictionary
        return str(list(code_dict.keys())[list(code_dict.values()).index(val)])
    except ValueError:

        return val


def error_count(df, error, photo=None, sound=None, other=None):
    codes = ['bkk_photo_status_code',
             'bkk_sound_status_code',
             'smev_exit_code_description']
    if photo:
        error = get_code_from_val(photo_code_dicts, error)
        mask = [True
                if pd.notna(each)
                and str(error) in str(each)
                else False
                for each in df[codes[0]]]
    elif sound:
        error = get_code_from_val(sound_code_dicts, error)
        mask = [True
                if pd.notna(each)
                and str(error) in str(each)
                else False
                for each in df[codes[1]]]
    # it is urgly :(
    elif other:
        try:
            error = get_code_from_val(other_code_dicts, error)
            mask = [True
                    if pd.notna(each)
                    # I can not understand why
                    # str(error) == str(each) not working
                    and float(error) == each
                    else False
                    for each in df['success']]
        except ValueError:
            o_error = re.search('[A-z]', error)
            if o_error is not None and o_error.group() == 'p':
                mask = [True
                        if each['success'] == 414
                        and each[codes[2]][-7:] == '\'photo\''
                        else False
                        for idx, each in df.iterrows()]
            elif o_error is not None and o_error.group() == 's':
                mask = [True
                        if each['success'] == 414
                        and each[codes[2]][-7:] == '\'sound\''
                        else False
                        for idx, each in df.iterrows()]
            else:
                mask = [True if pd.notna(each) and error == '{}'.format(each)
                        else False for each in df[codes[2]]]

    return df[mask].shape[0]


def form_table(df, columns):
    dates_list = list(set(str(datetime.strptime(each,
                                                '%Y.%m.%d %H:%M:%S').date())
                      for each in df['init_date_time']))

    table = {}

    table['Дата'] = dates_list+['Итого:']

    date_dfs = [df_by_dates(df, date) for date in dates_list]

    main_fields = []
    for date_df in date_dfs:
        all_regs = date_df.loc[date_df['bank'].notna()].shape[0]
        success_regs = date_df.loc[date_df['bank'].notna()].loc[
                (date_df['success'] == 200) |
                (date_df['success'] == 202)].shape[0]
        success_pers = success_regs/all_regs
        failed_regs = date_df.loc[date_df['bank'].notna()].loc[
                ((date_df['success'] != 200) &
                 (date_df['success'] != 202))].shape[0]
        failed_pers = failed_regs/all_regs
        main_fields.append([all_regs,
                            success_regs,
                            success_pers,
                            failed_regs,
                            failed_pers])
    main_fields = np.array(main_fields).T.tolist()

    for idx, field in enumerate(set_column_names(df)[0][1:]):
        table[field] = main_fields[idx]
        if (idx != 2) & (idx != 4):
            table[field] = table[field]+[sum(table[field])]
        elif (idx == 2):
            table[field] = table[field]+[
                    sum(table['Успешные регистрации']) /
                    sum(table['Всего попыток регистрации'])]
        elif (idx == 4):
            table[field] = table[field]+[
                    sum(table['Всего неуспешных']) /
                    sum(table['Всего попыток регистрации'])]

    for error in columns[1]:
        table[error] = [error_count(df_by_dates(df, date), error, photo=True)
                        for date in dates_list]
        table[error] = table[error]+[sum(table[error])]
    for error in columns[2]:
        table[error] = [error_count(df_by_dates(df, date), error, sound=True)
                        for date in dates_list]
        table[error] = table[error]+[sum(table[error])]
    for error in columns[3]:
        table[error] = [error_count(df_by_dates(df, date), error, other=True)
                        for date in dates_list]
        table[error] = table[error]+[sum(table[error])]

    return table


def total2df(df, columns):
    _columns = ['Банк'] + columns[0][1:]

    table = {}
    table['Банк'] = set_df_field(df, 'bank')

    total = np.array([], dtype=int)
    success = np.array([], dtype=int)
    fails = np.array([], dtype=int)
    for bank in table['Банк']:
        total = np.append(total, df.loc[df['bank'] == bank].shape[0])
        success = np.append(success, df.loc[df['bank'] == bank].loc[
                (df['success'] == 200) |
                (df['success'] == 202)].shape[0])
        fails = np.append(fails, df.loc[df['bank'] == bank].loc[
                ((df['success'] != 200) &
                 (df['success'] != 202))].shape[0])

    success_pers = success/total
    fails_pers = fails/total

    table[_columns[1]] = total
    table[_columns[2]] = success
    table[_columns[3]] = success_pers
    table[_columns[4]] = fails
    table[_columns[5]] = fails_pers

    for error in columns[1]:
        table[error] = [error_count(
                df.loc[df['bank'] == bank],
                error,
                photo=True)
            for bank in table['Банк']]
    for error in columns[2]:
        table[error] = [error_count(
                df.loc[df['bank'] == bank],
                error, sound=True)
            for bank in table['Банк']]
    for error in columns[3]:
        table[error] = [error_count(
                df.loc[df['bank'] == bank],
                error, other=True)
            for bank in table['Банк']]

    return table


def df2table(df, writer, bank):
    if bank == 'Общее':
        _df = df
    else:
        _df = df.loc[df['bank'] == bank]

    output_df = pd.DataFrame.from_dict(form_table(
            _df, set_column_names(df)))
    output_df = output_df.sort_values(by='Дата')

    if len(bank) >= 30:
        bank = bank[0:30]
    output_df.to_excel(writer, sheet_name=bank, index=False)

    workbook = writer.book
    worksheet = writer.sheets[bank]

    format0 = workbook.add_format({
            'border': 1,
            'text_wrap': True
            })
    format1 = workbook.add_format({'num_format': '0', 'border': 1})
    success_format1 = workbook.add_format({
            'num_format': '0',
            'border': 1,
            'fg_color': '#F0FFF0'
            })
    fails_format1 = workbook.add_format({
            'num_format': '0',
            'border': 1,
            'fg_color': '#FFF5EE'
            })

    success_format2 = workbook.add_format({
            'num_format': '0%',
            'border': 1,
            'fg_color': '#F0FFF0'
            })
    fails_format2 = workbook.add_format({
            'num_format': '0%',
            'border': 1,
            'fg_color': '#FFF5EE'
            })
    last_row_format1 = workbook.add_format({
            'border': 1,
            'fg_color': '#DCDCDC',
            'bold': True,
            'align': 'center',
            'num_format': '0'
            })
    last_row_format2 = workbook.add_format({
            'border': 1,
            'fg_color': '#DCDCDC',
            'bold': True,
            'align': 'center',
            'num_format': '0%'
            })
    header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'vcenter',
            'align': 'center',
            'fg_color': '#DCDCDC',
            'border': 1
            })

    worksheet.set_column('A:A', 18, format0)
    worksheet.set_column('B:B', 18, format1)
    worksheet.set_column('C:C', 18, success_format1)
    worksheet.set_column('D:D', 18, success_format2)
    worksheet.set_column('E:E', 18, fails_format1)
    worksheet.set_column('F:F', 18, fails_format2)
    worksheet.set_column(6, len(list(output_df))-1, 18, format1)

    for col_num, value in enumerate(output_df.columns.values):
        worksheet.write(0, col_num, value, header_format)
    for idx, value in enumerate(output_df.iloc[len(output_df)-1].values):
        if idx == 3 or idx == 5:
            worksheet.write(len(output_df), idx, value, last_row_format2)
        else:
            worksheet.write(len(output_df), idx, value, last_row_format1)

    # Success and not success registrations
    chart = workbook.add_chart({
            'type': 'pie'
            })
    chart.add_series({
            'values': '=(\'{0}\'!$D${1},\'{0}\'!$F${1})'.format(
                    bank, len(output_df)+1),
            'categories': '=(\'{0}\'!$D$1,\'{0}\'!$F$1)'.format(bank),
            'data_labels': {'value': True
                            }})
    chart.set_title({
            'name': 'Регистрации'
            })
    worksheet.insert_chart('A{}'.format(len(output_df)+2), chart)


# TODO need some cleverness of shorten
def rebank(bank):
    bank = [letter if letter != '"' else '' for letter in bank]
    return ''.join(bank)


def output_tables(df, args, folder=None):
    if not folder:
        folder = args.folder
    df = df_notna(df, 'bank')
    if df.shape[0] == 0:
        return None

    bank_list = set_df_field(df, 'bank')

    bank_list_full = [u'Общее']+bank_list
    for idx, bank in enumerate(bank_list_full):
        sys.stdout.write("\rGroup progress: {1}>".format(idx+1, "="*idx))
        writer = pd.ExcelWriter('reports/{0}/{1}_{2}.xlsx'.format(
                                folder,
                                rebank(bank), args.folder),
                                engine='xlsxwriter')
        df2table(df, writer, bank)
        writer.save()
        sys.stdout.flush()

    all_df = pd.DataFrame.from_dict(total2df(df, set_column_names(df)))
    all_df = all_df[all_df['Всего попыток регистрации'] != 0]
    all_df.to_excel('reports/{0}/banks_total.xls'.format(folder),
                    index=False)
    sys.stdout.write("\n")
