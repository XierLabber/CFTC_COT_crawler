import urllib.request
import zipfile
import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime

# 'c_year.txt'
url1 = 'https://www.cftc.gov/files/dea/history/com_disagg_txt_2024.zip'
url2 = 'https://www.cftc.gov/files/dea/history/com_disagg_txt_2023.zip'

# 'FinComYY.txt'
url3 = 'https://www.cftc.gov/files/dea/history/com_fin_txt_2023.zip'


def remove_file(file_path):
    try:
        # 删除文件
        os.remove(file_path)
        print(f"文件 '{file_path}' 已成功删除。")
    except FileNotFoundError:
        print(f"文件 '{file_path}' 不存在，无法删除。")
    except Exception as e:
        print("删除文件时发生错误:", e)


def get_df(url, unziped_file_name, tmp_folder = './tmp/', delete_after = True):
    destination_file = tmp_folder + url.split('/')[-1]
    
    # 设置自定义的 User-Agent 头部
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        # 下载文件
        req = urllib.request.Request(url, headers=headers)
        # 下载文件
        with urllib.request.urlopen(req) as response, open(destination_file, 'wb') as out_file:
            data = response.read()  # 读取响应数据
            out_file.write(data)    # 将数据写入到本地文件
        print("文件下载成功！")
    except Exception as e:
        print("文件下载失败:", e)
    
    extract_dir = tmp_folder
    try:
        # 确保解压缩目标路径存在
        if not os.path.exists(extract_dir):
            os.makedirs(extract_dir)

        # 打开 .zip 文件
        with zipfile.ZipFile(destination_file, 'r') as zip_ref:
            # 解压所有文件到指定目录下
            zip_ref.extractall(extract_dir)
        print("文件解压成功！")
    except Exception as e:
        print("文件解压失败:", e)
    
    file_path = tmp_folder + unziped_file_name
    df = None
    try:
        # 读取以空格分隔的文本文件，创建 DataFrame
        df = pd.read_csv(file_path)
        # 显示 DataFrame
    except FileNotFoundError:
        print(f"文件 '{file_path}' 不存在。")
    except Exception as e:
        print("发生了错误:", e)
    
    if delete_after:
        remove_file(destination_file)
        remove_file(file_path)
    
    return df

def get_diff(ori_df: pd.DataFrame, new_df: pd.DataFrame):
    date_row_name = 'As_of_Date_In_Form_YYMMDD'
    all_dates = set(ori_df[date_row_name])
    return new_df[~new_df[date_row_name].isin(all_dates)]

def work_with_url(url, unziped_file_name, download_target_folder = './download/', tmp_folder = './tmp/', delete_after = True):
    df = get_df(url, unziped_file_name, tmp_folder, delete_after)
    grouped = df.groupby('Market_and_Exchange_Names')

    # 遍历分组对象，并输出每个分组的 DataFrame
    for group_name, group_df in grouped:
        group_name = group_name.split(' - ')[0]
    
        file_name = 'CFTC_COT_' + group_name + '.csv'   # 要检查的文件名
        file_name = file_name.replace('/', '-')

        # 构建文件的完整路径
        file_path = os.path.join(download_target_folder, file_name)
        # 检查文件是否存在
        if os.path.exists(file_path):
            try:
                existing_df = pd.read_csv(file_path)
                unexists_rows = get_diff(existing_df, group_df)
                unexists_rows.iloc[:, 'created_at'] = datetime.now()
                unexists_rows.iloc[:, 'updated_at'] = datetime.now()
                updated_df = pd.concat([existing_df, unexists_rows], ignore_index=True)
                updated_df.to_csv(file_path, index=False)
                print(f"DataFrame 已成功追加到文件 '{file_name}' 中。")
            except Exception as e:
                print("创建文件时发生错误:", e)
        else:
            with open(file_path, 'w'):
                pass
            group_df['created_at'] = datetime.now()
            group_df['updated_at'] = datetime.now()
            group_df.to_csv(file_path, index=False)


def work():
    download_path = './download/'
    file_list = os.listdir(download_path)
    # 获取当前的年份
    current_year = datetime.now().year
    all_files = []
    if len(file_list) == 0:
        all_files = [(f'https://www.cftc.gov/files/dea/history/com_disagg_txt_{i}.zip', 'c_year.txt') for i in range(2010, current_year + 1)] + [(f'https://www.cftc.gov/files/dea/history/com_fin_txt_{i}.zip', 'FinComYY.txt') for i in range(2010, current_year + 1)]
    else:
        all_files = [(f'https://www.cftc.gov/files/dea/history/com_disagg_txt_{current_year}.zip', 'c_year.txt')] + [(f'https://www.cftc.gov/files/dea/history/com_fin_txt_{current_year}.zip', 'FinComYY.txt')]
    for url, unziped_file_name in tqdm(all_files):
        work_with_url(url, unziped_file_name)

work()
