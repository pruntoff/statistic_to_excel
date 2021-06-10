# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from datetime import datetime
import re
import sys
import waves

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

    - form_table(df, columns): return dict you can use to output to the file;
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
bank_names = pd.read_excel('data/dicts/bank_names.xlsx', header=None)
photo_code_dicts = dict(zip(photo_code[1], photo_code[0]))
sound_code_dicts = dict(zip(sound_code[1], sound_code[0]))
other_code_dicts = dict(zip(other_code[1], other_code[0]))
bank_names_dict = dict(zip(bank_names[1], bank_names[0]))
bank_names_dict = {str(k): v for k, v in bank_names_dict.items()}


# Service functions
# I like useless things. Why not?
def replace_433(series):
    for idx, each in enumerate(series):
        if isinstance(each, str):
            try:
                true_value = [int(i) for i in each.split(' ') if int(i) != 433]
                if len(true_value) > 1 or len(true_value) == 0:
                    pass
                else:
                    series = series.replace(each, true_value[0])
            except ValueError:
                try:
                    true_value = [int(i)
                                  for i in each.split('\n') if int(i) != 433]
                    if len(true_value) > 1:
                        pass
                    else:
                        series = series.replace(each, true_value[0])
                except ValueError:
                    pass
    return series


def double_error_match(series):
    if len(series['success'].split(' ')) > 1:
        return series['success'].split(' ')
    elif len(series['success'].split('\n')) > 1:
        return series['success'].split('\n')


def define_reg(reg):
    if int(reg[8]) == 200 or int(reg[8]) == 202:
        reg[10:] = [np.nan for i in range(len(reg[10:]))]
        return reg
    if int(reg[8]) != 200 or int(reg[8]) != 202:
        return reg


def replace_double_error(df):
    for idx, row in df.iterrows():
        if isinstance(row['success'], str) and len(row['success']) > 3 and re.match('[0-9]', row['success']):
            double_error = double_error_match(row)
            if '200' in double_error or '202' in double_error:
                first_reg, second_reg = (row.tolist(), row.tolist())
                first_reg[8] = double_error.pop(0)
                second_reg[8] = double_error.pop(0)
                first_reg = define_reg(first_reg)
                second_reg = define_reg(second_reg)
                df.append([first_reg, second_reg], ignore_index=True)
                df = df.drop(idx)
            else:
                pass
        else:
            pass
    return df


def clear_df(df):
    df = df.loc[df['info_system'] != '-']
    df = rewrite_bank_names(df)
    df = df.loc[[False if each == '413\n500' else True for each in df['success'].tolist()]]
    df = df.loc[[False if each == '413\n414' else True for each in df['success'].tolist()]]
    df = df.loc[[False if each == '200\n406' else True for each in df['success'].tolist()]]
    #df = df.loc[df['success'] != '413\n500']
    #df = df.loc[df['success'] != '413\n414']
    #df = df.loc[df['success'] != '200\n406']
    df['success'] = replace_433(df['success'])
    df = replace_double_error(df)
    df['success'] = df['success'].replace('SMEV_FAIL', 505)
    df['success'] = pd.to_numeric(
        df['success'],
        downcast='integer',
        errors='coerce').fillna(df['success'])
    return df


def third_wave():
    two_waves = waves.fs_banks+['ПАО  "СБЕРБАНК РОССИИ"']
    third_wave_banks = [value
                        for key, value
                        in bank_names_dict.items()
                        if value not in two_waves]
    return third_wave_banks


def rewrite_bank_names(df):
    new_names = [bank_names_dict[mnemonic]
                 for mnemonic
                 in df['info_system'].tolist()]
    df['bank'] = new_names
    return df


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
    dt = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
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
    perrors = [each for sublist in perrors for each in sublist if each != '0' and each != 0 and each != '\\N']
    perrors_name = [photo_code_dicts[int(error)] for error in set(perrors)]

    sound_codes = set_df_field(df, codes[1])
    serrors = [error_format(each) for each in sound_codes]
    serrors = [each for sublist in serrors for each in sublist if each != '0' or 0]
    serrors_name = [sound_code_dicts[int(error)] for error in sound_codes if error != '0' and error != 0 and error != '\\N']

    filtered_df = df_notna(df, 'success')
    _filtered_df = filtered_df.loc[(filtered_df['success'] != 200) &
                                   (filtered_df['success'] != 202) &
                                   (filtered_df['success'] != 414)]
    other_codes = set_df_field(_filtered_df, 'success')
    oerrors_name = [other_code_dicts[int(error)] for error in set(other_codes)]
    _filtered_df = filtered_df.loc[(filtered_df['success'] == 505)]
    error505 = list(set(['{}'.format(error)
                    for error in _filtered_df['smev_exit_code_description']]))
    oerrors_name = oerrors_name + error505
    _filtered_df = filtered_df.loc[(filtered_df['success'] == 414)]
    error414 = list(set([error
                         for error
                         in _filtered_df['smev_exit_code_description']]))
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
        return str(int(list(code_dict.keys())[list(code_dict.values()).index(val)]))
    except ValueError:

        return val


def error_count(df, error, photo=None, sound=None, other=None):
    codes = ['bkk_photo_status_code',
             'bkk_sound_status_code',
             'smev_exit_code_description']
    if photo:
        error = get_code_from_val(photo_code_dicts, error)
        try:
            mask = [True
                    if pd.notna(each)
            and each != "\\N"
                    and int(error) == int(each)
                    else False
                    for each in df[codes[0]]]
        except ValueError:
            mask = [True
                    if pd.notna(each)
                       and each != "\\N"
                       and error in each.strip().split(',') #each can be '1048576,2097152'
                    else False
                    for each in df[codes[0]]]
    elif sound:
        error = get_code_from_val(sound_code_dicts, error)
        try:
            mask = [True
                    if pd.notna(each)
                       and each != "\\N"
                       and int(error) == int(each)
                    else False
                    for each in df[codes[1]]]
        except ValueError:
            mask = [True
                    if pd.notna(each)
                       and each != "\\N"
                       and error in each.strip().split(',')  # each can be '1048576,2097152'
                    else False
                    for each in df[codes[1]]]
    # it is urgly :(
    elif other:
        try:
            error = get_code_from_val(other_code_dicts, error)
            mask = [True
                    if pd.notna(each)
		    and each != "\\N"
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
                                                '%Y-%m-%d %H:%M:%S').date())
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

    #photo errors
    for error in columns[1]:
        table[error] = [error_count(df_by_dates(df, date), error, photo=True)
                        for date in dates_list]
        table[error] = table[error]+[sum(table[error])]
        #print(photo_code_dicts)
        #print(error, photo_code_dicts[error])

    #sound errors
    for error in columns[2]:
        table[error] = [error_count(df_by_dates(df, date), error, sound=True)
                        for date in dates_list]
        table[error] = table[error]+[sum(table[error])]
    #other errors
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
    output_df.to_excel(writer, sheet_name=bank, index=False, na_rep='NA')

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
                                engine='xlsxwriter', options={'nan_inf_to_errors': True})
        df2table(df, writer, bank)
        writer.save()
        sys.stdout.flush()

    all_df = pd.DataFrame.from_dict(total2df(df, set_column_names(df)))
    all_df = all_df[all_df['Всего попыток регистрации'] != 0]
    all_df.to_excel('reports/{0}/banks_total.xlsx'.format(folder),
                    index=False, na_rep='NA')
    sys.stdout.write("\n")


def bank_detailed(df, args, folder=None):
    if not folder:
        folder = args.folder

    old_col_names = [
            'init_date_time',
            'request_id',
            'service_centre',
            'success',
            'bkk_photo_status_code',
            'bkk_sound_status_code',
            'smev_exit_code_description'
            ]
    new_col_names = [
            'Время начала регистрации в ЕБС',
            'Идентификатор транзакции',
            'Центр Обслуживания',
            'Успешность',
            'Ошибка БКК по модальности фото',
            'Ошибка БКК по модальности звук',
            'Возвращенный код СМЭВ'
            ]
    col_zip = list(zip(old_col_names, new_col_names))
    col_dict = {k: v for k, v in col_zip}
    new_success = ['Успешно'
                   if each == 200 or each == 202
                   else 'Неуспешно'
                   for each in df['success'].tolist()]


    new_photo_code = []
    for error in df['bkk_photo_status_code']:
        try:
            if (pd.notna(error) and error != '\\N'):
                new_photo_code.append(photo_code_dicts[int(error)])
            else:
                new_photo_code.append('')
        except ValueError:
            if (pd.notna(error) and error != '\\N'):
                error_list = error.strip().split(',')
                text_list = [photo_code_dicts[int(e)] for e in error_list]
                new_photo_code.append(' | '.join(text_list))
            else:
                new_photo_code.append('')

    new_sound_code = []
    for error in df['bkk_sound_status_code']:
        try:
            if (pd.notna(error) and error != '\\N'):
                new_sound_code.append(sound_code_dicts[int(error)])
            else:
                new_sound_code.append('')
        except ValueError:
            if (pd.notna(error) and error != '\\N'):
                error_list = error.strip().split(',')
                text_list = [sound_code_dicts[int(e)] for e in error_list]
                new_sound_code.append(' | '.join(text_list))
            else:
                new_sound_code.append('')

    output_df = df.loc[:, old_col_names]
    output_df['success'] = new_success
    output_df['bkk_photo_status_code'] = new_photo_code
    output_df['bkk_sound_status_code'] = new_sound_code
    output_df = output_df.rename(columns=col_dict)
    output_df.to_excel('reports/{0}/{1}.xlsx'.format(folder, folder),
                       index=False)
