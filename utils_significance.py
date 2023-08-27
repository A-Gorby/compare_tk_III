import pandas as pd
# from utils_significance import calc_significance_01, calc_significance_serv_uet, calc_fraction, filter_most_of_fraction_sum
def calc_significance_01( section, df, col_freq, col_multiplicity):
    # col_significance = 'Частота_Кратность' + '_' + section
    col_significance = 'Частота_Кратность'
    df[col_significance] = df[col_freq] * df[col_multiplicity]
    df.sort_values([col_significance], ascending=False, inplace=True)
    return df, col_significance
def calc_significance_serv_uet( df, col_freq, col_multiplicity, col_uet1, col_uet2):
    """  Услуги УЕТ """
    # print("calc_significance_serv_uet:", df.columns)
    col_significance = 'Частота_Кратность_Услуги_УЕТ'
    df[col_significance] = df[col_freq] * df[col_multiplicity] * (df[col_uet1] *2 + df[col_uet2])
    return df, col_significance
def calc_fraction(df, col_to_calc, suffix='_fraction'):
    # print("calc_fraction:", df.columns)
    col_fractions = col_to_calc + suffix
    if (df[col_to_calc].dtype==float) or (df[col_to_calc].dtype==int):
        pass
    elif (df[col_to_calc].dtype==object) or (df[col_to_calc].dtype==str):
        try:
            df[col_to_calc] = df[col_to_calc].astype(float)
        except Exception as err:
            print(f"'{col_to_calc}' is not number dtype")
            print(err)
            return df, col_fractions
    else:
        print(f"'{col_to_calc}' is not number dtype")
        return df, col_fractions
    sum = df[col_to_calc].sum()
    
    df[col_fractions] = df[col_to_calc]/sum

    return df, col_fractions
def filter_most_of_fraction_sum(df, col_fraction, threshold_significance_sum):
    # print(f"filter_most_of_fraction_sum: col_fraction: {col_fraction}, threshold_significance_sum: {threshold_significance_sum}")
    try:
        acc_sum = df[col_fraction].sum()
    except Exception as err:
        print(f"Колонка '{col_fraction}' содержит вместо доли от общей суммы ошибочные значения")
        print(err)
        return None
    if (acc_sum > 1.05) or (acc_sum < .95):
        print(f"Сумма по Колонке '{col_fraction}' не равна 1.0")
    df = df.sort_values([col_fraction], ascending=False)
    n_dict = []
    acc = 0
    # threshold_significance_sum = .9
    for i_row, row in df.iterrows():
        if acc + row[col_fraction] <= threshold_significance_sum:
            acc += row[col_fraction]
            n_dict.append(row)
        else:
            break
    dfn = pd.DataFrame(n_dict)
    return dfn
def calc_significance(cmp_sections, df_cmp1, df_cmp2):
    col_freq, col_multiplicity, col_uet1, col_uet2 = 'Усредненная частота предоставления', 'Усредненная кратность применения', 'УЕТ 1', 'УЕТ 2'
    df_cmp1_n, df_cmp2_n = [], []
    df_cmp_n = []
    for i_d, df_cmp in enumerate([df_cmp1, df_cmp2]):
        df_cmp_n.append([])
        for i_s, section in enumerate(cmp_sections):
            # print(df_cmp[i_s].columns)
            df_l, col_significance = calc_significance_01( section, df_cmp[i_s], col_freq, col_multiplicity)
            # print("df_l.columns:", df_l.columns)
            df_l, col_fractions = calc_fraction(df_l, col_significance, suffix='_fraction')
            if section=='Услуги':
                df_l, col_significance_serv = calc_significance_serv_uet( df_l, col_freq, col_multiplicity, col_uet1, col_uet2)
                df_l, col_fractions_serv = calc_fraction(df_l, col_significance_serv, suffix='_fraction')
            df_cmp_n[i_d].append(df_l)
    
    df_cmp1_n, df_cmp2_n = df_cmp_n[0], df_cmp_n[1]
    # return df_cmp1_n, df_cmp2_n, col_significance, col_fractions, col_significance_serv, col_fractions_serv  
    return df_cmp1_n, df_cmp2_n
