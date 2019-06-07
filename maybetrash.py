# -*- coding: utf-8 -*-



# not used
def tableit(name, bank_list, multiple=True):
    if multiple:
        for bank in bank_list:
            writer = pd.ExcelWriter('reports/{0}/{1}.xlsx'.format(
                    name, rebank(bank)), engine='xlsxwriter')
            df2table(bank)
            writer.save()
    else:
        writer = pd.ExcelWriter('report_01-07_all.xlsx'.format(
                rebank(bank)), engine='xlsxwriter')
        for bank in bank_list:
            df2table(bank)
        writer.save()

def set_column_names(df):
    column_names = ['Дата', 'Всего попыток регистрации', 'Успешные регистрации', '% успешных', 'Всего неуспешных', '% неуспешных']

    photo_codes = list(set(df[df['bkk_photo_status_code'].notna()]['bkk_photo_status_code'].tolist()))
    #should be fixed, matches only '111, 222 111, 222' strings
    perrors = [each.split(',')[0::2] for each in photo_codes]
    perrors = [each for sublist in perrors for each in sublist if each != '0']
    perrors_name = [photo_code_dicts[int(error)] for error in set(perrors)]

    sound_codes = list(set(df[df['bkk_sound_status_code'].notna()]['bkk_sound_status_code'].tolist()))
    serrors_name = [sound_code_dicts[int(error)] for error in set(sound_codes)]

    other_codes = list(set(df.loc[(df['success'] != '200') & (df['success'] != '202') & (df['success'].notna())]['success'].tolist()))
    oerrors_name = [other_code_dicts[int(error)] for error in set(other_codes)]
    return (column_names, perrors_name, serrors_name, oerrors_name)

if args.wave and args.wave == '1':
    df = df.loc[(df['bank'].isin(waves.first_wave_banks))]
    folder = '/{0}/{1}_first_wave'.format(args.folder,args.folder)
elif args.wave and args.wave == '2':
    df = df.loc[(df['bank'].isin(waves.second_wave_banks))]
    folder = '/{0}/{1}_second_wave'.format(args.folder,args.folder)
elif args.wave and args.wave == 'upd':
    df = df.loc[(df['bank'].isin(waves.updated_banks))]
    folder = '/{0}/{1}_upd'.format(args.folder,args.folder)
elif args.wave and args.wave == 'not_upd':
    df = df.loc[(df['bank'].isin(waves.not_updated_banks))]
    folder = '/{0}/{1}_not_upd'.format(args.folder,args.folder)
elif args.wave and args.wave == 'all_not_upd':
    df = df.loc[(df['bank'].isin(waves.all_not_updated_banks))]
    folder = '/{0}/{1}all_not_upd'.format(args.folder,args.folder)
elif not args.wave:
    pass