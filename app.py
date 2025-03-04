import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from openai import OpenAI
import os
import tempfile
import traceback
from io import BytesIO
import time
import logging
from datetime import datetime

# ロギングの設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Streamlitの設定
st.set_page_config(page_title="広告パフォーマンス分析ダッシュボード", layout="wide")

# セッション状態の初期化
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = None
if 'cv_contribution' not in st.session_state:
    st.session_state.cv_contribution = None
if 'cpa_factors' not in st.session_state:
    st.session_state.cpa_factors = None
if 'llm_analysis' not in st.session_state:
    st.session_state.llm_analysis = None
if 'upload_error' not in st.session_state:
    st.session_state.upload_error = None
if 'campaign_data' not in st.session_state:
    st.session_state.campaign_data = None
if 'campaign_analysis' not in st.session_state:
    st.session_state.campaign_analysis = None
if 'adgroup_data' not in st.session_state:
    st.session_state.adgroup_data = None
if 'adgroup_analysis' not in st.session_state:
    st.session_state.adgroup_analysis = None
if 'auto_analysis_results' not in st.session_state:
    st.session_state.auto_analysis_results = None
if 'important_media' not in st.session_state:
    st.session_state.important_media = None

import os
old_http_proxy = os.environ.pop('HTTP_PROXY', None)
old_https_proxy = os.environ.pop('HTTPS_PROXY', None)

import httpx
http_client = httpx.Client(
    base_url="https://api.openai.com",
    follow_redirects=True,
    timeout=60.0,
)

# タイトルとイントロダクション
st.title("広告パフォーマンス分析ダッシュボード")
st.markdown("""
このダッシュボードでは、広告パフォーマンスデータを分析し、以下の情報を提供します：
- 全体サマリー（主要KPI比較）
- CV増減の寄与度分析
- CPA変化要因分析
- 分析エージェントによる詳細分析
""")

# サイドバー - 設定
st.sidebar.header("設定")

# APIキー入力（省略可能）
api_key = st.sidebar.text_input("OpenAI API Key (オプション)", type="password")

if "HTTP_PROXY" in os.environ:
    del os.environ["HTTP_PROXY"]
if "HTTPS_PROXY" in os.environ:
    del os.environ["HTTPS_PROXY"]


# 使用するモデルの選択
model_options = ["gpt-4-turbo", "gpt-4", "gpt-3.5-turbo"]
selected_model = st.sidebar.selectbox("使用するモデル", model_options, index=0)

# 月の選択
with st.sidebar.expander("分析期間の設定", expanded=True):
    current_month = st.selectbox("当月", ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06", "2024-07", "2024-08"], index=6)
    previous_month = st.selectbox("前月", ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06", "2024-07", "2024-08"], index=5)

# 分析エージェント設定
with st.sidebar.expander("分析エージェント設定", expanded=True):
    analysis_mode = st.radio("分析モード", ["手動", "自動"], index=0)
    
    if analysis_mode == "自動":
        analysis_depth = st.select_slider(
            "分析の深さ", 
            options=["媒体レベル", "キャンペーンレベル", "広告グループレベル"], 
            value="キャンペーンレベル"
        )
        importance_threshold = st.slider("重要度閾値 (%)", 10, 50, 30)
        max_items_to_analyze = st.slider("分析する最大項目数", 1, 10, 3)
    
    # 手動モードの設定
    else:
        show_campaign_analysis = st.checkbox("キャンペーンレベル分析を有効化", value=True)
        show_adgroup_analysis = st.checkbox("広告グループレベル分析を有効化", value=False)

# データ処理関数
def preprocess_data(df):
    """アップロードされたExcelデータを前処理する関数"""
    try:
        logger.info("データ前処理を開始")
        
        # 日付列のフォーマット確認とログ出力
        logger.info(f"日付カラムの型: {df['day'].dtype}")
        logger.info(f"日付カラムの最初の値: {df['day'].iloc[0]} (型: {type(df['day'].iloc[0])})")
        
        # 日付列を修正 - 様々な入力形式に対応
        if pd.api.types.is_datetime64_any_dtype(df['day']):
            # Timestamp型の場合
            logger.info("Timestamp型の日付を処理")
            df['yearMonth'] = df['day'].dt.strftime("%Y-%m")
        else:
            # 文字列型の場合、フォーマット検出
            sample_date = str(df['day'].iloc[0])
            logger.info(f"文字列型の日付を処理: サンプル '{sample_date}'")
            
            if '/' in sample_date:
                # '1/1/24' 形式の場合
                parts = sample_date.split('/')
                if len(parts) == 3 and len(parts[2]) == 2:
                    # '1/1/24' 形式
                    logger.info("短い年フォーマット (MM/DD/YY) を検出")
                    df['yearMonth'] = df['day'].apply(lambda x: f"20{x.split('/')[2]}-{x.split('/')[0].zfill(2)}")
                else:
                    # '1/1/2024' 形式
                    logger.info("完全な年フォーマット (MM/DD/YYYY) を検出")
                    df['yearMonth'] = df['day'].apply(lambda x: f"{x.split('/')[2]}-{x.split('/')[0].zfill(2)}")
            elif '-' in sample_date:
                # '2024-01-01' 形式の場合
                logger.info("ISO形式の日付を検出 (YYYY-MM-DD)")
                df['yearMonth'] = df['day'].apply(lambda x: x.split('-')[0] + '-' + x.split('-')[1])
            else:
                # その他の形式 - 日付に変換してから処理
                logger.info("日付形式を自動変換")
                try:
                    date_series = pd.to_datetime(df['day'], errors='coerce')
                    df['yearMonth'] = date_series.dt.strftime("%Y-%m")
                except Exception as date_err:
                    logger.error(f"日付変換エラー: {date_err}")
                    # フォールバック - 何も分からない場合はそのまま使用
                    df['yearMonth'] = df['day']
        
        # 数値のクリーニング - より柔軟に
        for col in ['impression', 'click', 'cost', 'cv']:
            if df[col].dtype == 'object':  # 文字列の場合
                df[col] = df[col].replace({',': '', '¥': '', ' ': ''}, regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 派生指標の計算 - ゼロ除算対策
        df['ctr'] = np.where(df['impression'] > 0, df['click'] / df['impression'] * 100, 0)
        df['cvr'] = np.where(df['click'] > 0, df['cv'] / df['click'] * 100, 0)
        df['cpc'] = np.where(df['click'] > 0, df['cost'] / df['click'], 0)
        df['cpa'] = np.where(df['cv'] > 0, df['cost'] / df['cv'], 0)
        df['cpm'] = np.where(df['impression'] > 0, df['cost'] / df['impression'] * 1000, 0)
        
        # 無限値やNaNをNoneに置き換え
        df = df.replace([np.inf, -np.inf], np.nan)
        
        # データの概要をログ出力
        logger.info(f"処理後のデータ概要:\n{df.describe().to_string()}")
        logger.info(f"yearMonth列の値例: {df['yearMonth'].unique()[:5]}")
        
        logger.info("データ前処理完了")
        return df
    except Exception as e:
        logger.error(f"データ処理中にエラーが発生しました: {e}")
        logger.error(traceback.format_exc())
        return None

# Excel読み込み関数
def load_excel_file(uploaded_file):
    """様々な形式のExcelファイルを安全に読み込む関数"""
    try:
        # まずバイトデータとして読み込み
        bytes_data = uploaded_file.getvalue()
        logger.info(f"読み込んだバイト数: {len(bytes_data)}")
        
        # 最初は文字列として読み込みを試みる
        try:
            df = pd.read_excel(BytesIO(bytes_data), dtype={
                'day': str,
                'media': str,
                'campaign': str,  # キャンペーン列を追加
                'adgroup': str,   # 広告グループ列を追加
                'impression': str,
                'click': str,
                'cost': str,
                'cv': str
            }, engine='openpyxl')
            logger.info("文字列としてExcelを読み込みました")
        except Exception as string_err:
            logger.warning(f"文字列としての読み込みに失敗: {string_err}")
            # フォールバック: デフォルト設定で読み込み
            df = pd.read_excel(BytesIO(bytes_data))
            logger.info("デフォルト設定でExcelを読み込みました")
        
        # データフレームの基本情報
        logger.info(f"データフレームの形状: {df.shape}")
        logger.info(f"カラム: {df.columns.tolist()}")
        logger.info(f"データ型: \n{df.dtypes}")
        
        # 必要なカラムがない場合はダミーカラムを追加（分析エージェント対応）
        if 'campaign' not in df.columns:
            df['campaign'] = 'unknown_campaign'
            logger.info("'campaign'カラムがないため、ダミーカラムを追加しました")
        
        if 'adgroup' not in df.columns:
            df['adgroup'] = 'unknown_adgroup'
            logger.info("'adgroup'カラムがないため、ダミーカラムを追加しました")
        
        return df
    except Exception as e:
        logger.error(f"Excel読み込み中にエラーが発生しました: {e}")
        logger.error(traceback.format_exc())
        return None

# 媒体レベルの分析関数
def calculate_cv_contribution(df, previous_month, current_month):
    """CV増減の寄与度を計算する関数"""
    try:
        # 月別・メディア別に集計
        month_media_data = df.groupby(['yearMonth', 'media']).agg({
            'impression': 'sum',
            'click': 'sum',
            'cost': 'sum',
            'cv': 'sum'
        }).reset_index()
        
        # 前月と当月のデータを抽出
        prev_data = month_media_data[month_media_data['yearMonth'] == previous_month]
        curr_data = month_media_data[month_media_data['yearMonth'] == current_month]
        
        # 全メディアを取得
        all_media = list(set(prev_data['media'].tolist() + curr_data['media'].tolist()))
        
        # CV変化と寄与率の計算
        contribution_data = []
        
        # 全体のCV変化を計算
        total_cv_prev = prev_data['cv'].sum()
        total_cv_curr = curr_data['cv'].sum()
        total_cv_change = total_cv_curr - total_cv_prev
        
        for media in all_media:
            cv_prev = prev_data[prev_data['media'] == media]['cv'].sum() if not prev_data[prev_data['media'] == media].empty else 0
            cv_curr = curr_data[curr_data['media'] == media]['cv'].sum() if not curr_data[curr_data['media'] == media].empty else 0
            cv_change = cv_curr - cv_prev
            
            # 寄与率の計算
            contribution_rate = (cv_change / total_cv_change * 100) if total_cv_change != 0 else 0
            
            contribution_data.append({
                'media': media,
                'cv_prev': cv_prev,
                'cv_curr': cv_curr,
                'cv_change': cv_change,
                'contribution_rate': contribution_rate
            })
        
        # 寄与率の絶対値で降順ソート
        contribution_df = pd.DataFrame(contribution_data)
        contribution_df = contribution_df.sort_values(by='contribution_rate', key=abs, ascending=False)
        
        return contribution_df, total_cv_prev, total_cv_curr, total_cv_change
    
    except Exception as e:
        logger.error(f"CV寄与度計算中にエラーが発生しました: {e}")
        logger.error(traceback.format_exc())
        return None, 0, 0, 0

def calculate_cpa_factors(df, previous_month, current_month):
    """CPA変化の要因を分析する関数"""
    try:
        # 月別・メディア別に集計
        month_media_data = df.groupby(['yearMonth', 'media']).agg({
            'impression': 'sum',
            'click': 'sum',
            'cost': 'sum',
            'cv': 'sum'
        }).reset_index()
        
        # 派生指標を再計算
        month_media_data['ctr'] = month_media_data['click'] / month_media_data['impression'] * 100
        month_media_data['cvr'] = month_media_data['cv'] / month_media_data['click'] * 100
        month_media_data['cpc'] = month_media_data['cost'] / month_media_data['click']
        month_media_data['cpa'] = month_media_data['cost'] / month_media_data['cv']
        month_media_data['cpm'] = month_media_data['cost'] / month_media_data['impression'] * 1000
        
        # 前月と当月のデータを抽出
        prev_data = month_media_data[month_media_data['yearMonth'] == previous_month]
        curr_data = month_media_data[month_media_data['yearMonth'] == current_month]
        
        # 全メディアを取得（CV > 0のものに限定）
        prev_media_with_cv = prev_data[prev_data['cv'] > 0]['media'].tolist()
        curr_media_with_cv = curr_data[curr_data['cv'] > 0]['media'].tolist()
        all_media_with_cv = list(set(prev_media_with_cv + curr_media_with_cv))
        
        # CPA変化要因分析
        cpa_factors_data = []
        
        for media in all_media_with_cv:
            prev_row = prev_data[prev_data['media'] == media]
            curr_row = curr_data[curr_data['media'] == media]
            
            # 両月にデータがあり、CVも存在する場合のみ分析
            if not prev_row.empty and not curr_row.empty and prev_row['cv'].values[0] > 0 and curr_row['cv'].values[0] > 0:
                # 値を取得
                cpa_prev = prev_row['cpa'].values[0]
                cpa_curr = curr_row['cpa'].values[0]
                cvr_prev = prev_row['cvr'].values[0]
                cvr_curr = curr_row['cvr'].values[0]
                cpc_prev = prev_row['cpc'].values[0]
                cpc_curr = curr_row['cpc'].values[0]
                cpm_prev = prev_row['cpm'].values[0]
                cpm_curr = curr_row['cpm'].values[0]
                ctr_prev = prev_row['ctr'].values[0]
                ctr_curr = curr_row['ctr'].values[0]
                
                # 変化率を計算
                cpa_change = (cpa_curr - cpa_prev) / cpa_prev * 100
                cvr_change = (cvr_curr - cvr_prev) / cvr_prev * 100
                cpc_change = (cpc_curr - cpc_prev) / cpc_prev * 100
                cpm_change = (cpm_curr - cpm_prev) / cpm_prev * 100
                ctr_change = (ctr_curr - ctr_prev) / ctr_prev * 100
                
                # 主要因と副要因を判定
                primary_factor = "CVR" if abs(cvr_change) > abs(cpc_change) else "CPC"
                secondary_factor = None
                if primary_factor == "CPC":
                    secondary_factor = "CPM" if abs(cpm_change) > abs(ctr_change) else "CTR"
                
                # データを保存
                cpa_factors_data.append({
                    'media': media,
                    'cpa_prev': cpa_prev,
                    'cpa_curr': cpa_curr,
                    'cpa_change': cpa_change,
                    'cvr_prev': cvr_prev,
                    'cvr_curr': cvr_curr,
                    'cvr_change': cvr_change,
                    'cpc_prev': cpc_prev,
                    'cpc_curr': cpc_curr,
                    'cpc_change': cpc_change,
                    'cpm_prev': cpm_prev,
                    'cpm_curr': cpm_curr,
                    'cpm_change': cpm_change,
                    'ctr_prev': ctr_prev,
                    'ctr_curr': ctr_curr,
                    'ctr_change': ctr_change,
                    'primary_factor': primary_factor,
                    'secondary_factor': secondary_factor
                })
        
        # CPA変化率でソート
        cpa_factors_df = pd.DataFrame(cpa_factors_data)
        if not cpa_factors_df.empty:
            cpa_factors_df = cpa_factors_df.sort_values(by='cpa_change', ascending=False)
        
        return cpa_factors_df
    
    except Exception as e:
        logger.error(f"CPA要因分析中にエラーが発生しました: {e}")
        logger.error(traceback.format_exc())
        return None

def calculate_summary_stats(df, previous_month, current_month):
    """全体の集計統計を計算する関数"""
    try:
        # 月別に集計
        month_data = df.groupby('yearMonth').agg({
            'impression': 'sum',
            'click': 'sum',
            'cost': 'sum',
            'cv': 'sum'
        }).reset_index()
        
        # 派生指標を再計算
        month_data['ctr'] = month_data['click'] / month_data['impression'] * 100
        month_data['cvr'] = month_data['cv'] / month_data['click'] * 100
        month_data['cpc'] = month_data['cost'] / month_data['click']
        month_data['cpa'] = month_data['cost'] / month_data['cv']
        month_data['cpm'] = month_data['cost'] / month_data['impression'] * 1000
        
        # 前月と当月のデータを抽出
        prev_data = month_data[month_data['yearMonth'] == previous_month]
        curr_data = month_data[month_data['yearMonth'] == current_month]
        
        # データが存在するか確認
        if prev_data.empty or curr_data.empty:
            return None
        
        # 1行に変換
        prev_row = prev_data.iloc[0]
        curr_row = curr_data.iloc[0]
        
        # 変化を計算
        summary_data = {
            'impression': {'prev': prev_row['impression'], 'curr': curr_row['impression'], 
                          'diff': curr_row['impression'] - prev_row['impression'], 
                          'pct_change': (curr_row['impression'] - prev_row['impression']) / prev_row['impression'] * 100},
            'click': {'prev': prev_row['click'], 'curr': curr_row['click'], 
                     'diff': curr_row['click'] - prev_row['click'], 
                     'pct_change': (curr_row['click'] - prev_row['click']) / prev_row['click'] * 100},
            'cost': {'prev': prev_row['cost'], 'curr': curr_row['cost'], 
                    'diff': curr_row['cost'] - prev_row['cost'], 
                    'pct_change': (curr_row['cost'] - prev_row['cost']) / prev_row['cost'] * 100},
            'cv': {'prev': prev_row['cv'], 'curr': curr_row['cv'], 
                  'diff': curr_row['cv'] - prev_row['cv'], 
                  'pct_change': (curr_row['cv'] - prev_row['cv']) / prev_row['cv'] * 100},
            'ctr': {'prev': prev_row['ctr'], 'curr': curr_row['ctr'], 
                   'diff': curr_row['ctr'] - prev_row['ctr'], 
                   'pct_change': (curr_row['ctr'] - prev_row['ctr']) / prev_row['ctr'] * 100},
            'cvr': {'prev': prev_row['cvr'], 'curr': curr_row['cvr'], 
                   'diff': curr_row['cvr'] - prev_row['cvr'], 
                   'pct_change': (curr_row['cvr'] - prev_row['cvr']) / prev_row['cvr'] * 100},
            'cpc': {'prev': prev_row['cpc'], 'curr': curr_row['cpc'], 
                   'diff': curr_row['cpc'] - prev_row['cpc'], 
                   'pct_change': (curr_row['cpc'] - prev_row['cpc']) / prev_row['cpc'] * 100},
            'cpa': {'prev': prev_row['cpa'], 'curr': curr_row['cpa'], 
                   'diff': curr_row['cpa'] - prev_row['cpa'], 
                   'pct_change': (curr_row['cpa'] - prev_row['cpa']) / prev_row['cpa'] * 100}
        }
        
        return summary_data
    
    except Exception as e:
        logger.error(f"全体統計計算中にエラーが発生しました: {e}")
        logger.error(traceback.format_exc())
        return None

# 分析エージェント関数 - キャンペーンレベル分析
def get_campaign_level_analysis(df, selected_media, previous_month, current_month):
    """指定された媒体のキャンペーンレベル分析を行う関数"""
    try:
        # 指定された媒体のデータのみをフィルタリング
        media_df = df[df['media'] == selected_media].copy()
        
        # 月別・キャンペーン別に集計
        month_campaign_data = media_df.groupby(['yearMonth', 'campaign']).agg({
            'impression': 'sum',
            'click': 'sum',
            'cost': 'sum',
            'cv': 'sum'
        }).reset_index()
        
        # 派生指標を計算
        month_campaign_data['ctr'] = month_campaign_data['click'] / month_campaign_data['impression'] * 100
        month_campaign_data['cvr'] = month_campaign_data['cv'] / month_campaign_data['click'] * 100
        month_campaign_data['cpc'] = month_campaign_data['cost'] / month_campaign_data['click']
        month_campaign_data['cpa'] = month_campaign_data['cost'] / month_campaign_data['cv']
        
        # 前月と当月のデータを抽出
        prev_data = month_campaign_data[month_campaign_data['yearMonth'] == previous_month]
        curr_data = month_campaign_data[month_campaign_data['yearMonth'] == current_month]
        
        # 全キャンペーンを取得
        all_campaigns = list(set(prev_data['campaign'].tolist() + curr_data['campaign'].tolist()))
        
        # 比較データを作成
        campaign_data = []
        
        # 媒体全体のCV変化を計算（寄与率計算用）
        total_cv_prev = prev_data['cv'].sum()
        total_cv_curr = curr_data['cv'].sum()
        total_cv_change = total_cv_curr - total_cv_prev
        
        for campaign in all_campaigns:
            prev_row = prev_data[prev_data['campaign'] == campaign]
            curr_row = curr_data[curr_data['campaign'] == campaign]
            
            # 前月または当月のデータがない場合にデフォルト値を設定
            cv_prev = prev_row['cv'].sum() if not prev_row.empty else 0
            cv_curr = curr_row['cv'].sum() if not curr_row.empty else 0
            cv_change = cv_curr - cv_prev
            
            # 寄与率の計算
            contribution_rate = (cv_change / total_cv_change * 100) if total_cv_change != 0 else 0
            
            # CPA計算（0除算対策）
            cpa_prev = prev_row['cost'].sum() / cv_prev if not prev_row.empty and cv_prev > 0 else 0
            cpa_curr = curr_row['cost'].sum() / cv_curr if not curr_row.empty and cv_curr > 0 else 0
            cpa_change = ((cpa_curr - cpa_prev) / cpa_prev * 100) if cpa_prev > 0 and cpa_curr > 0 else 0
            
            # コスト
            cost_prev = prev_row['cost'].sum() if not prev_row.empty else 0
            cost_curr = curr_row['cost'].sum() if not curr_row.empty else 0
            cost_change = ((cost_curr - cost_prev) / cost_prev * 100) if cost_prev > 0 else 0
            
            # CVR計算
            click_prev = prev_row['click'].sum() if not prev_row.empty else 0
            click_curr = curr_row['click'].sum() if not curr_row.empty else 0
            cvr_prev = (cv_prev / click_prev * 100) if click_prev > 0 else 0
            cvr_curr = (cv_curr / click_curr * 100) if click_curr > 0 else 0
            cvr_change = ((cvr_curr - cvr_prev) / cvr_prev * 100) if cvr_prev > 0 else 0
            
            campaign_data.append({
                'campaign': campaign,
                'cv_prev': cv_prev,
                'cv_curr': cv_curr,
                'cv_change': cv_change,
                'contribution_rate': contribution_rate,
                'cpa_prev': cpa_prev,
                'cpa_curr': cpa_curr,
                'cpa_change': cpa_change,
                'cost_prev': cost_prev,
                'cost_curr': cost_curr,
                'cost_change': cost_change,
                'cvr_prev': cvr_prev,
                'cvr_curr': cvr_curr,
                'cvr_change': cvr_change
            })
        
        # 寄与率の絶対値で降順ソート
        campaign_df = pd.DataFrame(campaign_data)
        if not campaign_df.empty:
            campaign_df = campaign_df.sort_values(by='contribution_rate', key=abs, ascending=False)
        
        return campaign_df, total_cv_prev, total_cv_curr, total_cv_change
    
    except Exception as e:
        logger.error(f"キャンペーン分析中にエラーが発生しました: {e}")
        logger.error(traceback.format_exc())
        return None, 0, 0, 0

# 広告グループレベル分析
def get_adgroup_level_analysis(df, selected_media, selected_campaign, previous_month, current_month):
    """指定された媒体とキャンペーンの広告グループレベル分析を行う関数"""
    try:
        # 指定された媒体とキャンペーンのデータをフィルタリング
        filtered_df = df[(df['media'] == selected_media) & (df['campaign'] == selected_campaign)].copy()
        
        # 月別・広告グループ別に集計
        month_adgroup_data = filtered_df.groupby(['yearMonth', 'adgroup']).agg({
            'impression': 'sum',
            'click': 'sum',
            'cost': 'sum',
            'cv': 'sum'
        }).reset_index()
        
        # 派生指標を計算
        month_adgroup_data['ctr'] = month_adgroup_data['click'] / month_adgroup_data['impression'] * 100
        month_adgroup_data['cvr'] = month_adgroup_data['cv'] / month_adgroup_data['click'] * 100
        month_adgroup_data['cpc'] = month_adgroup_data['cost'] / month_adgroup_data['click']
        month_adgroup_data['cpa'] = month_adgroup_data['cost'] / month_adgroup_data['cv']
        
        # 前月と当月のデータを抽出
        prev_data = month_adgroup_data[month_adgroup_data['yearMonth'] == previous_month]
        curr_data = month_adgroup_data[month_adgroup_data['yearMonth'] == current_month]
        
        # 全広告グループを取得
        all_adgroups = list(set(prev_data['adgroup'].tolist() + curr_data['adgroup'].tolist()))
        
        # 比較データを作成
        adgroup_data = []
        
        # キャンペーン全体のCV変化を計算（寄与率計算用）
        total_cv_prev = prev_data['cv'].sum()
        total_cv_curr = curr_data['cv'].sum()
        total_cv_change = total_cv_curr - total_cv_prev
        
        for adgroup in all_adgroups:
            prev_row = prev_data[prev_data['adgroup'] == adgroup]
            curr_row = curr_data[curr_data['adgroup'] == adgroup]
            
            # 前月または当月のデータがない場合にデフォルト値を設定
            cv_prev = prev_row['cv'].sum() if not prev_row.empty else 0
            cv_curr = curr_row['cv'].sum() if not curr_row.empty else 0
            cv_change = cv_curr - cv_prev
            
            # 寄与率の計算
            contribution_rate = (cv_change / total_cv_change * 100) if total_cv_change != 0 else 0
            
            # CPA計算（0除算対策）
            cpa_prev = prev_row['cost'].sum() / cv_prev if not prev_row.empty and cv_prev > 0 else 0
            cpa_curr = curr_row['cost'].sum() / cv_curr if not curr_row.empty and cv_curr > 0 else 0
            cpa_change = ((cpa_curr - cpa_prev) / cpa_prev * 100) if cpa_prev > 0 and cpa_curr > 0 else 0
            
            # コスト
            cost_prev = prev_row['cost'].sum() if not prev_row.empty else 0
            cost_curr = curr_row['cost'].sum() if not curr_row.empty else 0
            cost_change = ((cost_curr - cost_prev) / cost_prev * 100) if cost_prev > 0 else 0
            
            # CVR計算
            click_prev = prev_row['click'].sum() if not prev_row.empty else 0
            click_curr = curr_row['click'].sum() if not curr_row.empty else 0
            cvr_prev = (cv_prev / click_prev * 100) if click_prev > 0 else 0
            cvr_curr = (cv_curr / click_curr * 100) if click_curr > 0 else 0
            cvr_change = ((cvr_curr - cvr_prev) / cvr_prev * 100) if cvr_prev > 0 else 0
            
            adgroup_data.append({
                'adgroup': adgroup,
                'cv_prev': cv_prev,
                'cv_curr': cv_curr,
                'cv_change': cv_change,
                'contribution_rate': contribution_rate,
                'cpa_prev': cpa_prev,
                'cpa_curr': cpa_curr,
                'cpa_change': cpa_change,
                'cost_prev': cost_prev,
                'cost_curr': cost_curr,
                'cost_change': cost_change,
                'cvr_prev': cvr_prev,
                'cvr_curr': cvr_curr,
                'cvr_change': cvr_change
            })
        
        # 寄与率の絶対値で降順ソート
        adgroup_df = pd.DataFrame(adgroup_data)
        if not adgroup_df.empty:
            adgroup_df = adgroup_df.sort_values(by='contribution_rate', key=abs, ascending=False)
        
        return adgroup_df, total_cv_prev, total_cv_curr, total_cv_change
    
    except Exception as e:
        logger.error(f"広告グループ分析中にエラーが発生しました: {e}")
        logger.error(traceback.format_exc())
        return None, 0, 0, 0

# 重要媒体の特定
def identify_important_media(cv_contribution_df, cpa_factors_df, threshold=30, max_items=3):
    """重要な媒体を特定する関数"""
    try:
        important_media = []
        
        # CV寄与率が大きい媒体を特定
        cv_important = cv_contribution_df[abs(cv_contribution_df['contribution_rate']) >= threshold].copy()
        
        # CPA変化率が大きい媒体を特定
        cpa_important = None
        if cpa_factors_df is not None and not cpa_factors_df.empty:
            cpa_important = cpa_factors_df[abs(cpa_factors_df['cpa_change']) >= threshold].copy()
        
        # 重要媒体のリストを作成
        media_scores = {}
        
        # CV寄与率によるスコア
        for _, row in cv_important.iterrows():
            media = row['media']
            score = abs(row['contribution_rate'])
            reason = f"CV寄与率が{row['contribution_rate']:.1f}%"
            
            if media not in media_scores:
                media_scores[media] = {
                    'score': 0,
                    'reasons': []
                }
            
            media_scores[media]['score'] += score
            media_scores[media]['reasons'].append(reason)
        
        # CPA変化率によるスコア
        if cpa_important is not None:
            for _, row in cpa_important.iterrows():
                media = row['media']
                score = abs(row['cpa_change'])
                reason = f"CPA変化率が{row['cpa_change']:.1f}%"
                
                if media not in media_scores:
                    media_scores[media] = {
                        'score': 0,
                        'reasons': []
                    }
                
                media_scores[media]['score'] += score
                media_scores[media]['reasons'].append(reason)
        
        # スコア順に並べ替え
        media_list = [{'name': media, 'importance_score': data['score'], 'reason': ', '.join(data['reasons'])} 
                    for media, data in media_scores.items()]
        media_list.sort(key=lambda x: x['importance_score'], reverse=True)
        
        # 上位N件を返す
        return media_list[:max_items]
    
    except Exception as e:
        logger.error(f"重要媒体特定中にエラーが発生しました: {e}")
        logger.error(traceback.format_exc())
        return []

# LLM分析関数 - OpenAIバージョン
def run_llm_analysis(summary_data, cv_contribution_df, cpa_factors_df, previous_month, current_month):
    """OpenAI APIを使用して広告パフォーマンスデータを分析する関数"""
    try:
        # APIキーチェック
        if not api_key:
            return "LLM分析を実行するにはOpenAIのAPIキーが必要です。サイドバーでAPIキーを入力してください。"
        
        # OpenAI APIクライアントの設定
        client = OpenAI(
          api_key=api_key,
          http_client=http_client
        )
        # データを整形してプロンプトを作成
        prompt = f"""あなたは広告データ分析の専門家です。以下の広告パフォーマンスデータ（{previous_month}と{current_month}の比較）を分析し、洞察と推奨事項を提供してください。

## 1. 全体サマリー
| 指標 | {previous_month} | {current_month} | 差分 | 変化率 |
|------|-----|-----|------|--------|
| インプレッション | {summary_data['impression']['prev']:,.0f} | {summary_data['impression']['curr']:,.0f} | {summary_data['impression']['diff']:,.0f} | {summary_data['impression']['pct_change']:.1f}% |
| クリック数 | {summary_data['click']['prev']:,.0f} | {summary_data['click']['curr']:,.0f} | {summary_data['click']['diff']:,.0f} | {summary_data['click']['pct_change']:.1f}% |
| コスト | {summary_data['cost']['prev']:.0f}円 | {summary_data['cost']['curr']:.0f}円 | {summary_data['cost']['diff']:.0f}円 | {summary_data['cost']['pct_change']:.1f}% |
| CV数 | {summary_data['cv']['prev']:.0f} | {summary_data['cv']['curr']:.0f} | {summary_data['cv']['diff']:.0f} | {summary_data['cv']['pct_change']:.1f}% |
| CTR | {summary_data['ctr']['prev']:.1f}% | {summary_data['ctr']['curr']:.1f}% | {summary_data['ctr']['diff']:.1f}% | {summary_data['ctr']['pct_change']:.1f}% |
| CVR | {summary_data['cvr']['prev']:.1f}% | {summary_data['cvr']['curr']:.1f}% | {summary_data['cvr']['diff']:.1f}% | {summary_data['cvr']['pct_change']:.1f}% |
| CPA | {summary_data['cpa']['prev']:.0f}円 | {summary_data['cpa']['curr']:.0f}円 | {summary_data['cpa']['diff']:.0f}円 | {summary_data['cpa']['pct_change']:.1f}% |

## 2. CV増減の寄与度ランキング（上位10件）
"""
        
        # CV寄与度ランキングを追加
        prompt += "| 順位 | メディア | 前月CV | 当月CV | CV変化 | 寄与率 |\n|------|--------|--------|--------|--------|--------|\n"
        for i, row in cv_contribution_df.head(10).iterrows():
            prompt += f"| {i+1} | {row['media']} | {row['cv_prev']:.0f} | {row['cv_curr']:.0f} | {row['cv_change']:.0f} | {row['contribution_rate']:.1f}% |\n"
        
        # CPA要因分析を追加
        prompt += "\n## 3. CPA変化要因分析\n"
        prompt += "| メディア | 前月CPA | 当月CPA | CPA変化率 | 主要因 | 詳細 |\n|--------|--------|--------|-----------|--------|------|\n"
        for i, row in cpa_factors_df.head(10).iterrows():
            secondary_info = ""
            if row['primary_factor'] == "CPC":
                secondary_info = f"({row['secondary_factor']}が副因: {row['cpm_change' if row['secondary_factor'] == 'CPM' else 'ctr_change']:.1f}%)"
            prompt += f"| {row['media']} | {row['cpa_prev']:.0f}円 | {row['cpa_curr']:.0f}円 | {row['cpa_change']:.1f}% | {row['primary_factor']} | {row['primary_factor']}変化: {row['cvr_change'] if row['primary_factor'] == 'CVR' else row['cpc_change']:.1f}% {secondary_info} |\n"
        
        # 分析指示を追加
        prompt += f"""
以上のデータに基づいて、以下の分析をMarkdown形式で提供してください：

1. 全体傾向の要約（3-5つの主要ポイント）
2. CV増減における重要な変化と傾向
3. CPA変化要因の主要パターンと傾向
4. 戦略的変化の解釈（メディア間の予算シフトなど）
5. 3つの重要な課題と3つの重要な機会（データに基づく根拠と推奨アクション）

回答は簡潔で具体的な洞察を含め、マーケティング担当者が理解しやすいよう専門用語は適切に説明してください。"""

        # LLMでの分析実行
        with st.spinner("LLMによる分析を実行中..."):
            logger.info(f"OpenAI API呼び出し開始: モデル={selected_model}")
            response = client.chat.completions.create(
                model=selected_model,
                messages=[
                    {"role": "system", "content": "あなたは広告パフォーマンス分析の専門家です。データに基づいた具体的で実用的な洞察と推奨事項を提供してください。回答はマークダウン形式で、タイトル、見出し、リスト、表などを適切に使用してください。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
            )
            logger.info("OpenAI API呼び出し完了")
            
            return response.choices[0].message.content
    
    except Exception as e:
        logger.error(f"LLM分析中にエラーが発生しました: {e}")
        logger.error(traceback.format_exc())
        return f"LLM分析中にエラーが発生しました: {e}"

# キャンペーンレベルのLLM分析
def run_campaign_analysis(campaign_df, selected_media, previous_month, current_month):
    """キャンペーンレベルのLLM分析を実行する関数"""
    try:
        # APIキーチェック
        if not api_key:
            return "LLM分析を実行するにはOpenAIのAPIキーが必要です。サイドバーでAPIキーを入力してください。"
        
        # OpenAI APIクライアントの設定
        client = OpenAI(api_key=api_key)
        
        # キャンペーンデータを整形
        prompt = f"""あなたは広告データ分析の専門家です。以下の「{selected_media}」媒体のキャンペーンレベルのデータ（{previous_month}と{current_month}の比較）を分析し、洞察と推奨事項を提供してください。

## キャンペーンレベルのCV寄与度ランキング（上位10件）
"""
        
        # キャンペーン寄与度ランキングを追加
        prompt += "| 順位 | キャンペーン | 前月CV | 当月CV | CV変化 | 寄与率 | CPA変化率 | CVR変化率 |\n|------|------------|--------|--------|--------|--------|-----------|----------|\n"
        for i, row in campaign_df.head(10).iterrows():
            prompt += f"| {i+1} | {row['campaign']} | {row['cv_prev']:.0f} | {row['cv_curr']:.0f} | {row['cv_change']:.0f} | {row['contribution_rate']:.1f}% | {row['cpa_change']:.1f}% | {row['cvr_change']:.1f}% |\n"
        
        # 分析指示を追加
        prompt += f"""
以上のデータに基づいて、以下の分析をMarkdown形式で提供してください：

1. 「{selected_media}」媒体内での主要キャンペーンの動向（2-3つの重要ポイント）
2. 顕著な変化があったキャンペーンとその要因
3. キャンペーンレベルでの最適化提案（2-3つ）

回答は簡潔で具体的な洞察を含め、200-300単語程度に収めてください。マーケティング担当者が理解しやすいよう専門用語は適切に説明してください。"""

        # LLMでの分析実行
        with st.spinner(f"{selected_media}のキャンペーン分析を実行中..."):
            logger.info(f"キャンペーン分析のOpenAI API呼び出し開始: モデル={selected_model}")
            response = client.chat.completions.create(
                model=selected_model,
                messages=[
                    {"role": "system", "content": "あなたは広告パフォーマンス分析の専門家です。データに基づいた具体的で実用的な洞察と推奨事項を提供してください。回答はマークダウン形式で、タイトル、見出し、リスト、表などを適切に使用してください。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
            )
            logger.info("キャンペーン分析のOpenAI API呼び出し完了")
            
            return response.choices[0].message.content
    
    except Exception as e:
        logger.error(f"キャンペーン分析中にエラーが発生しました: {e}")
        logger.error(traceback.format_exc())
        return f"キャンペーン分析中にエラーが発生しました: {e}"

# 広告グループレベルのLLM分析
def run_adgroup_analysis(adgroup_df, selected_media, selected_campaign, previous_month, current_month):
    """広告グループレベルのLLM分析を実行する関数"""
    try:
        # APIキーチェック
        if not api_key:
            return "LLM分析を実行するにはOpenAIのAPIキーが必要です。サイドバーでAPIキーを入力してください。"
        
        # OpenAI APIクライアントの設定
        client = OpenAI(api_key=api_key)
        
        # 広告グループデータを整形
        prompt = f"""あなたは広告データ分析の専門家です。以下の「{selected_media}」媒体の「{selected_campaign}」キャンペーン内の広告グループレベルのデータ（{previous_month}と{current_month}の比較）を分析し、洞察と推奨事項を提供してください。

## 広告グループレベルのCV寄与度ランキング（上位10件）
"""
        
        # 広告グループ寄与度ランキングを追加
        prompt += "| 順位 | 広告グループ | 前月CV | 当月CV | CV変化 | 寄与率 | CPA変化率 | CVR変化率 |\n|------|------------|--------|--------|--------|--------|-----------|----------|\n"
        for i, row in adgroup_df.head(10).iterrows():
            prompt += f"| {i+1} | {row['adgroup']} | {row['cv_prev']:.0f} | {row['cv_curr']:.0f} | {row['cv_change']:.0f} | {row['contribution_rate']:.1f}% | {row['cpa_change']:.1f}% | {row['cvr_change']:.1f}% |\n"
        
        # 分析指示を追加
        prompt += f"""
以上のデータに基づいて、以下の分析をMarkdown形式で提供してください：

1. 広告グループレベルでの主要な変化（1-2つの重要ポイント）
2. 最も注目すべき広告グループとその理由
3. 広告グループレベルでの具体的な最適化アクション（1-2つ）

回答は簡潔で具体的な洞察を含め、150-200単語程度に収めてください。マーケティング担当者が理解しやすいよう専門用語は適切に説明してください。"""

        # LLMでの分析実行
        with st.spinner(f"{selected_media}の{selected_campaign}の広告グループ分析を実行中..."):
            logger.info(f"広告グループ分析のOpenAI API呼び出し開始: モデル={selected_model}")
            response = client.chat.completions.create(
                model=selected_model,
                messages=[
                    {"role": "system", "content": "あなたは広告パフォーマンス分析の専門家です。データに基づいた具体的で実用的な洞察と推奨事項を提供してください。回答はマークダウン形式で、タイトル、見出し、リスト、表などを適切に使用してください。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
            )
            logger.info("広告グループ分析のOpenAI API呼び出し完了")
            
            return response.choices[0].message.content
    
    except Exception as e:
        logger.error(f"広告グループ分析中にエラーが発生しました: {e}")
        logger.error(traceback.format_exc())
        return f"広告グループ分析中にエラーが発生しました: {e}"

# 自動分析エージェント
def run_auto_analysis(cv_contribution_df, cpa_factors_df, df, previous_month, current_month, threshold=30, max_items=3, depth="キャンペーンレベル"):
    """自動分析エージェントを実行する関数"""
    try:
        # APIキーチェック
        if not api_key:
            return "自動分析を実行するにはOpenAIのAPIキーが必要です。サイドバーでAPIキーを入力してください。"
        
        # 重要媒体の特定
        important_media = identify_important_media(cv_contribution_df, cpa_factors_df, threshold, max_items)
        
        # 分析結果を格納する辞書
        analysis_results = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'important_media': important_media,
            'media_analyses': {}
        }
        
        # 重要媒体ごとに分析
        for media_info in important_media:
            media_name = media_info['name']
            media_score = media_info['importance_score']
            media_reason = media_info['reason']
            
            # キャンペーンレベルの分析
            campaign_df = None
            campaign_analysis = None
            
            if depth in ["キャンペーンレベル", "広告グループレベル"]:
                # キャンペーンレベルのデータを取得
                campaign_df, _, _, _ = get_campaign_level_analysis(df, media_name, previous_month, current_month)
                
                if campaign_df is not None and not campaign_df.empty:
                    # キャンペーンレベルのLLM分析を実行
                    campaign_analysis = run_campaign_analysis(campaign_df, media_name, previous_month, current_month)
            
            # 広告グループレベルの分析
            adgroup_analyses = {}
            
            if depth == "広告グループレベル" and campaign_df is not None and not campaign_df.empty:
                # 重要なキャンペーンを特定（最大2つ）
                important_campaigns = campaign_df.head(2)['campaign'].tolist()
                
                for campaign in important_campaigns:
                    # 広告グループレベルのデータを取得
                    adgroup_df, _, _, _ = get_adgroup_level_analysis(df, media_name, campaign, previous_month, current_month)
                    
                    if adgroup_df is not None and not adgroup_df.empty:
                        # 広告グループレベルのLLM分析を実行
                        adgroup_analysis = run_adgroup_analysis(adgroup_df, media_name, campaign, previous_month, current_month)
                        adgroup_analyses[campaign] = {
                            'data': adgroup_df,
                            'analysis': adgroup_analysis
                        }
            
            # 媒体の分析結果を保存
            analysis_results['media_analyses'][media_name] = {
                'importance_score': media_score,
                'reason': media_reason,
                'campaign_data': campaign_df,
                'campaign_analysis': campaign_analysis,
                'adgroup_analyses': adgroup_analyses
            }
        
        return analysis_results, important_media
    
    except Exception as e:
        logger.error(f"自動分析中にエラーが発生しました: {e}")
        logger.error(traceback.format_exc())
        return None, []

# セーフモードでファイルアップロード処理
try:
    st.subheader("ファイルアップロード")
    st.markdown("広告パフォーマンスデータ（Excel）をアップロードしてください。")
    
    # ファイルアップロードウィジェット
    uploaded_file = st.file_uploader("広告パフォーマンスデータ（Excel）をアップロード", type=["xlsx"])
    
    # ファイル情報の表示（デバッグ用）
    if uploaded_file is not None:
        st.write("ファイル情報:")
        file_details = {
            "ファイル名": uploaded_file.name,
            "ファイルタイプ": uploaded_file.type,
            "ファイルサイズ": f"{uploaded_file.size} バイト"
        }
        st.json(file_details)
        
        try:
            # Excelデータをメモリ上で読み込みテスト
            with st.spinner("ファイル読み込み中..."):
                # 改良された安全なExcel読み込み関数を使用
                df = load_excel_file(uploaded_file)
                
                if df is not None:
                    st.write("最初の5行:")
                    st.dataframe(df.head())
                    
                    # データ処理
                    processed_df = preprocess_data(df)
                    
                    if processed_df is not None:
                        st.session_state.processed_data = processed_df
                        
                        # 全体サマリーの計算
                        summary_data = calculate_summary_stats(processed_df, previous_month, current_month)
                        
                        # CV寄与度の計算
                        cv_contribution_df, total_cv_prev, total_cv_curr, total_cv_change = calculate_cv_contribution(
                            processed_df, previous_month, current_month
                        )
                        st.session_state.cv_contribution = cv_contribution_df
                        
                        # CPA変化要因の分析
                        cpa_factors_df = calculate_cpa_factors(processed_df, previous_month, current_month)
                        st.session_state.cpa_factors = cpa_factors_df
                        
                        st.session_state.analysis_results = summary_data
                        
                        # 自動分析モードの場合は自動分析を実行
                        if analysis_mode == "自動":
                            auto_results, important_media = run_auto_analysis(
                                cv_contribution_df, 
                                cpa_factors_df, 
                                processed_df, 
                                previous_month, 
                                current_month,
                                importance_threshold,
                                max_items_to_analyze,
                                analysis_depth
                            )
                            
                            st.session_state.auto_analysis_results = auto_results
                            st.session_state.important_media = important_media
                
                else:
                    st.error("ファイルの読み込みに失敗しました。")
    
        except Exception as e:
            st.error(f"ファイル処理中にエラーが発生しました: {e}")
            logger.error(f"ファイル処理中にエラーが発生しました: {e}")
            logger.error(traceback.format_exc())
            st.session_state.upload_error = str(e)

except Exception as e:
    st.error(f"予期せぬエラーが発生しました: {e}")
    logger.error(f"予期せぬエラーが発生しました: {e}")
    logger.error(traceback.format_exc())

# 分析結果の表示
if st.session_state.processed_data is not None and st.session_state.analysis_results is not None:
    
    # レイアウト：2カラムレイアウト
    left_column, right_column = st.columns([3, 3])
    
    with left_column:
        st.subheader("1. 全体サマリー")
        
        summary_data = st.session_state.analysis_results
        
        # 表形式でデータを表示
        summary_df = pd.DataFrame({
            f'{previous_month}': [f"{summary_data['impression']['prev']:,.0f}", 
                              f"{summary_data['click']['prev']:,.0f}", 
                              f"{summary_data['cost']['prev']:,.0f}", 
                              f"{summary_data['cv']['prev']:.0f}", 
                              f"{summary_data['ctr']['prev']:.1f}%", 
                              f"{summary_data['cvr']['prev']:.1f}%", 
                              f"{summary_data['cpa']['prev']:,.0f}"],
            f'{current_month}': [f"{summary_data['impression']['curr']:,.0f}", 
                            f"{summary_data['click']['curr']:,.0f}", 
                            f"{summary_data['cost']['curr']:,.0f}", 
                            f"{summary_data['cv']['curr']:.0f}", 
                            f"{summary_data['ctr']['curr']:.1f}%", 
                            f"{summary_data['cvr']['curr']:.1f}%", 
                            f"{summary_data['cpa']['curr']:,.0f}"],
            '変化量': [f"{summary_data['impression']['diff']:,.0f}", 
                    f"{summary_data['click']['diff']:,.0f}", 
                    f"{summary_data['cost']['diff']:,.0f}", 
                    f"{summary_data['cv']['diff']:.0f}", 
                    f"{summary_data['ctr']['diff']:.1f}%", 
                    f"{summary_data['cvr']['diff']:.1f}%", 
                    f"{summary_data['cpa']['diff']:,.0f}"],
            '変化率': [f"{summary_data['impression']['pct_change']:.1f}%", 
                    f"{summary_data['click']['pct_change']:.1f}%", 
                    f"{summary_data['cost']['pct_change']:.1f}%", 
                    f"{summary_data['cv']['pct_change']:.1f}%", 
                    f"{summary_data['ctr']['pct_change']:.1f}%", 
                    f"{summary_data['cvr']['pct_change']:.1f}%", 
                    f"{summary_data['cpa']['pct_change']:.1f}%"]
        }, index=['インプレッション', 'クリック数', 'コスト', 'CV数', 'CTR', 'CVR', 'CPA'])
        
        st.table(summary_df)
        
        # CV・CPAの可視化
        fig = go.Figure()
        
        # CV変化
        fig.add_trace(go.Bar(
            x=[previous_month, current_month],
            y=[summary_data['cv']['prev'], summary_data['cv']['curr']],
            name='CV数',
            marker_color=['#1f77b4', '#1f77b4'],
            text=[f"{summary_data['cv']['prev']:.0f}", f"{summary_data['cv']['curr']:.0f}"],
            textposition='outside'
        ))
        
        # CPA変化を別軸で表示
        fig.add_trace(go.Scatter(
            x=[previous_month, current_month],
            y=[summary_data['cpa']['prev'], summary_data['cpa']['curr']],
            name='CPA',
            mode='lines+markers+text',
            marker=dict(color='#d62728', size=10),
            line=dict(color='#d62728', width=2),
            text=[f"¥{summary_data['cpa']['prev']:,.0f}", f"¥{summary_data['cpa']['curr']:,.0f}"],
            textposition='top center',
            yaxis='y2'
        ))
        
        # レイアウト設定
        fig.update_layout(
            title='CV数とCPAの推移',
            xaxis=dict(title=''),
            yaxis=dict(title='CV数', showgrid=True),
            yaxis2=dict(title='CPA (円)', overlaying='y', side='right', showgrid=False),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    with right_column:
        st.subheader("2. CV増減の寄与度分析")
        
        if st.session_state.cv_contribution is not None:
            cv_contribution_df = st.session_state.cv_contribution
            
            # 寄与度のグラフ表示（上位10件）
            fig = px.bar(
                cv_contribution_df.head(10),
                y='media',
                x='contribution_rate',
                color='contribution_rate',
                labels={'contribution_rate': '寄与率 (%)', 'media': 'メディア'},
                title=f'CV増減への寄与度ランキング（上位10件）',
                color_continuous_scale=px.colors.diverging.RdBu,
                color_continuous_midpoint=0,
                orientation='h'
            )
            
            fig.update_layout(height=500)
            fig.update_traces(texttemplate='%{x:.1f}%', textposition='outside')
            
            st.plotly_chart(fig, use_container_width=True)
            
            # データテーブル表示
            st.markdown("**CV増減の寄与度データ（上位10件）**")
            display_df = cv_contribution_df.head(10).copy()
            display_df.columns = ['メディア', '前月CV', '当月CV', 'CV変化', '寄与率']
            display_df['寄与率'] = display_df['寄与率'].map(lambda x: f"{x:.1f}%")
            st.dataframe(display_df, use_container_width=True)
    
    # CPA変化要因分析
    st.subheader("3. CPA変化要因分析")
    
    if st.session_state.cpa_factors is not None:
        cpa_factors_df = st.session_state.cpa_factors
        
        # 2カラムレイアウト
        col1, col2 = st.columns([3, 3])
        
        with col1:
            # CPA変化率のグラフ表示
            fig = px.bar(
                cpa_factors_df.head(10),
                y='media',
                x='cpa_change',
                color='cpa_change',
                labels={'cpa_change': 'CPA変化率 (%)', 'media': 'メディア'},
                title='CPA変化率ランキング（上位10件）',
                color_continuous_scale=px.colors.diverging.RdBu_r,
                color_continuous_midpoint=0,
                orientation='h'
            )
            
            fig.update_layout(height=500)
            fig.update_traces(texttemplate='%{x:.1f}%', textposition='outside')
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # CPA変化の主要因分布
            if not cpa_factors_df.empty:
                factors_counts = cpa_factors_df['primary_factor'].value_counts().reset_index()
                factors_counts.columns = ['factor', 'count']
                
                fig = px.pie(
                    factors_counts, 
                    values='count', 
                    names='factor',
                    title='CPA変化の主要因分布',
                    color='factor',
                    color_discrete_map={'CVR': '#1f77b4', 'CPC': '#ff7f0e'},
                    hole=0.3
                )
                
                fig.update_layout(height=500)
                fig.update_traces(textinfo='percent+label', pull=[0.05, 0])
                
                st.plotly_chart(fig, use_container_width=True)
        
        # データテーブル表示
        st.markdown("**CPA変化要因データ（上位10件）**")
        display_df = cpa_factors_df.head(10)[['media', 'cpa_prev', 'cpa_curr', 'cpa_change', 'primary_factor', 'cvr_change', 'cpc_change']].copy()
        display_df.columns = ['メディア', '前月CPA', '当月CPA', 'CPA変化率', '主要因', 'CVR変化率', 'CPC変化率']
        display_df['CPA変化率'] = display_df['CPA変化率'].map(lambda x: f"{x:.1f}%")
        display_df['CVR変化率'] = display_df['CVR変化率'].map(lambda x: f"{x:.1f}%")
        display_df['CPC変化率'] = display_df['CPC変化率'].map(lambda x: f"{x:.1f}%")
        st.dataframe(display_df, use_container_width=True)
    
    # 分析モードに応じて表示内容を切り替え
    if analysis_mode == "手動":
        # 手動分析モード - LLM分析セクション
        st.subheader("4. LLMによる広告パフォーマンス分析")
        
        # LLM分析ボタン
        if st.button("LLM分析を実行"):
            # LLM分析を実行
            llm_analysis = run_llm_analysis(
                st.session_state.analysis_results,
                st.session_state.cv_contribution,
                st.session_state.cpa_factors,
                previous_month, 
                current_month
            )
            
            st.session_state.llm_analysis = llm_analysis
        
        # LLM分析結果を表示
        if st.session_state.llm_analysis:
            st.markdown(st.session_state.llm_analysis)
        
        # キャンペーンレベル分析（手動モード）
        if show_campaign_analysis:
            st.subheader("5. キャンペーンレベル分析")
            
            # 分析する媒体を選択
            if st.session_state.cv_contribution is not None:
                media_options = st.session_state.cv_contribution['media'].unique().tolist()
                
                # キャンペーン分析のUI
                selected_media = st.selectbox("詳細分析する媒体を選択", media_options)
                
                if st.button(f"{selected_media}のキャンペーン分析を実行"):
                    with st.spinner(f"{selected_media}のキャンペーンデータを分析中..."):
                        # キャンペーンデータを取得
                        campaign_df, _, _, _ = get_campaign_level_analysis(
                            st.session_state.processed_data, 
                            selected_media, 
                            previous_month, 
                            current_month
                        )
                        
                        st.session_state.campaign_data = campaign_df
                        
                        # キャンペーンLLM分析を実行
                        campaign_analysis = run_campaign_analysis(
                            campaign_df, 
                            selected_media, 
                            previous_month, 
                            current_month
                        )
                        
                        st.session_state.campaign_analysis = campaign_analysis
                
                # キャンペーンデータと分析結果を表示
                if 'campaign_data' in st.session_state and st.session_state.campaign_data is not None:
                    # データテーブル表示
                    st.markdown(f"**{selected_media}のキャンペーンレベルデータ（上位10件）**")
                    display_df = st.session_state.campaign_data.head(10).copy()
                    display_df = display_df[['campaign', 'cv_prev', 'cv_curr', 'cv_change', 'contribution_rate', 'cpa_change', 'cvr_change']]
                    display_df.columns = ['キャンペーン', '前月CV', '当月CV', 'CV変化', '寄与率', 'CPA変化率', 'CVR変化率']
                    display_df['寄与率'] = display_df['寄与率'].map(lambda x: f"{x:.1f}%")
                    display_df['CPA変化率'] = display_df['CPA変化率'].map(lambda x: f"{x:.1f}%")
                    display_df['CVR変化率'] = display_df['CVR変化率'].map(lambda x: f"{x:.1f}%")
                    st.dataframe(display_df, use_container_width=True)
                    
                    # LLM分析結果を表示
                    if 'campaign_analysis' in st.session_state and st.session_state.campaign_analysis:
                        st.markdown("**キャンペーンレベルの分析結果**")
                        st.markdown(st.session_state.campaign_analysis)
                
                # 広告グループレベル分析（手動モード）
                if show_adgroup_analysis and 'campaign_data' in st.session_state and st.session_state.campaign_data is not None:
                    st.subheader("6. 広告グループレベル分析")
                    
                    # キャンペーンの選択
                    campaign_options = st.session_state.campaign_data['campaign'].unique().tolist()
                    selected_campaign = st.selectbox("詳細分析するキャンペーンを選択", campaign_options)
                    
                    if st.button(f"{selected_media}の{selected_campaign}の広告グループ分析を実行"):
                        with st.spinner(f"{selected_campaign}の広告グループデータを分析中..."):
                            # 広告グループデータを取得
                            adgroup_df, _, _, _ = get_adgroup_level_analysis(
                                st.session_state.processed_data, 
                                selected_media, 
                                selected_campaign, 
                                previous_month, 
                                current_month
                            )
                            
                            st.session_state.adgroup_data = adgroup_df
                            
                            # 広告グループLLM分析を実行
                            adgroup_analysis = run_adgroup_analysis(
                                adgroup_df, 
                                selected_media, 
                                selected_campaign, 
                                previous_month, 
                                current_month
                            )
                            
                            st.session_state.adgroup_analysis = adgroup_analysis
                    
                    # 広告グループデータと分析結果を表示
                    if 'adgroup_data' in st.session_state and st.session_state.adgroup_data is not None:
                        # データテーブル表示
                        st.markdown(f"**{selected_campaign}の広告グループレベルデータ（上位10件）**")
                        display_df = st.session_state.adgroup_data.head(10).copy()
                        display_df = display_df[['adgroup', 'cv_prev', 'cv_curr', 'cv_change', 'contribution_rate', 'cpa_change', 'cvr_change']]
                        display_df.columns = ['広告グループ', '前月CV', '当月CV', 'CV変化', '寄与率', 'CPA変化率', 'CVR変化率']
                        display_df['寄与率'] = display_df['寄与率'].map(lambda x: f"{x:.1f}%")
                        display_df['CPA変化率'] = display_df['CPA変化率'].map(lambda x: f"{x:.1f}%")
                        display_df['CVR変化率'] = display_df['CVR変化率'].map(lambda x: f"{x:.1f}%")
                        st.dataframe(display_df, use_container_width=True)
                        
                        # LLM分析結果を表示
                        if 'adgroup_analysis' in st.session_state and st.session_state.adgroup_analysis:
                            st.markdown("**広告グループレベルの分析結果**")
                            st.markdown(st.session_state.adgroup_analysis)
    
    else:
        # 自動分析モード - 自動分析結果の表示
        st.subheader("4. 分析エージェントによる自動分析")
        
        if st.button("自動分析を実行"):
            with st.spinner("自動分析を実行中..."):
                # 自動分析を実行
                auto_results, important_media = run_auto_analysis(
                    st.session_state.cv_contribution, 
                    st.session_state.cpa_factors, 
                    st.session_state.processed_data, 
                    previous_month, 
                    current_month,
                    importance_threshold,
                    max_items_to_analyze,
                    analysis_depth
                )
                
                st.session_state.auto_analysis_results = auto_results
                st.session_state.important_media = important_media
        
        # 自動分析結果を表示
        if 'auto_analysis_results' in st.session_state and st.session_state.auto_analysis_results:
            auto_results = st.session_state.auto_analysis_results
            important_media = st.session_state.important_media
            
            st.markdown(f"**分析日時: {auto_results['timestamp']}**")
            st.markdown("### 重要媒体の自動検出結果")
            
            for i, media_info in enumerate(important_media):
                media_name = media_info['name']
                with st.expander(f"重要媒体 {i+1}: {media_name} (重要度スコア: {media_info['importance_score']:.1f})", expanded=(i==0)):
                    st.markdown(f"**選定理由**: {media_info['reason']}")
                    
                    # 媒体分析結果
                    media_analysis = auto_results['media_analyses'][media_name]
                    
                    # キャンペーンレベルの結果
                    if media_analysis['campaign_data'] is not None:
                        st.markdown("#### キャンペーンレベル分析")
                        
                        # キャンペーンデータの表示
                        campaign_df = media_analysis['campaign_data'].head(10).copy()
                        campaign_df = campaign_df[['campaign', 'cv_prev', 'cv_curr', 'cv_change', 'contribution_rate', 'cpa_change', 'cvr_change']]
                        campaign_df.columns = ['キャンペーン', '前月CV', '当月CV', 'CV変化', '寄与率', 'CPA変化率', 'CVR変化率']
                        campaign_df['寄与率'] = campaign_df['寄与率'].map(lambda x: f"{x:.1f}%")
                        campaign_df['CPA変化率'] = campaign_df['CPA変化率'].map(lambda x: f"{x:.1f}%")
                        campaign_df['CVR変化率'] = campaign_df['CVR変化率'].map(lambda x: f"{x:.1f}%")
                        st.dataframe(campaign_df, use_container_width=True)
                        
                        # キャンペーン分析結果
                        if media_analysis['campaign_analysis']:
                            st.markdown(media_analysis['campaign_analysis'])
                    
                    # 広告グループレベルの結果
                    if media_analysis['adgroup_analyses'] and len(media_analysis['adgroup_analyses']) > 0:
                        st.markdown("#### 広告グループレベル分析")
                        
                        for campaign, adgroup_info in media_analysis['adgroup_analyses'].items():
                            with st.expander(f"キャンペーン: {campaign}"):
                                # 広告グループデータの表示
                                adgroup_df = adgroup_info['data'].head(10).copy()
                                adgroup_df = adgroup_df[['adgroup', 'cv_prev', 'cv_curr', 'cv_change', 'contribution_rate', 'cpa_change', 'cvr_change']]
                                adgroup_df.columns = ['広告グループ', '前月CV', '当月CV', 'CV変化', '寄与率', 'CPA変化率', 'CVR変化率']
                                adgroup_df['寄与率'] = adgroup_df['寄与率'].map(lambda x: f"{x:.1f}%")
                                adgroup_df['CPA変化率'] = adgroup_df['CPA変化率'].map(lambda x: f"{x:.1f}%")
                                adgroup_df['CVR変化率'] = adgroup_df['CVR変化率'].map(lambda x: f"{x:.1f}%")
                                st.dataframe(adgroup_df, use_container_width=True)
                                
                                # 広告グループ分析結果
                                if adgroup_info['analysis']:
                                    st.markdown(adgroup_info['analysis'])
            
            # 総合分析と推奨事項
            if len(important_media) > 0:
                st.markdown("### 総合分析と最適化提案")
                
                # 総合分析のためのLLM呼び出し
                if st.button("総合分析を生成"):
                    with st.spinner("総合分析を生成中..."):
                        try:
                            # OpenAI APIクライアントの設定
                            client = OpenAI(api_key=api_key)
                            
                            # 総合分析のプロンプト作成
                            prompt = f"""あなたは広告パフォーマンス分析の専門家です。以下のデータ分析結果を総合的に評価し、全体的な最適化提案をしてください。

## 重要媒体の分析サマリー
"""
                            
                            for i, media_info in enumerate(important_media):
                                media_name = media_info['name']
                                media_analysis = auto_results['media_analyses'][media_name]
                                
                                prompt += f"\n### 媒体{i+1}: {media_name}\n"
                                prompt += f"重要度: {media_info['importance_score']:.1f}、理由: {media_info['reason']}\n"
                                
                                if media_analysis['campaign_analysis']:
                                    prompt += f"\nキャンペーン分析:\n{media_analysis['campaign_analysis']}\n"
                                
                                if media_analysis['adgroup_analyses'] and len(media_analysis['adgroup_analyses']) > 0:
                                    for campaign, adgroup_info in media_analysis['adgroup_analyses'].items():
                                        if adgroup_info['analysis']:
                                            prompt += f"\n広告グループ分析 ({campaign}):\n{adgroup_info['analysis']}\n"
                            
                            prompt += f"""
以上の分析結果を踏まえて、全体的な最適化戦略を提案してください。
特に注目すべき点や優先的に取り組むべき施策を3-5つにまとめ、それぞれの期待効果についても言及してください。
回答はマークダウン形式で、タイトル、見出し、箇条書きを適切に使用してください。
"""
                            
                            # LLMでの総合分析の実行
                            response = client.chat.completions.create(
                                model=selected_model,
                                messages=[
                                    {"role": "system", "content": "あなたは広告パフォーマンス分析の専門家です。データに基づいた具体的で実用的な洞察と推奨事項を提供してください。"},
                                    {"role": "user", "content": prompt}
                                ],
                                temperature=0.2,
                            )
                            
                            # 総合分析結果を表示
                            st.markdown(response.choices[0].message.content)
                            
                        except Exception as e:
                            st.error(f"総合分析の生成中にエラーが発生しました: {e}")
                            logger.error(f"総合分析の生成中にエラーが発生しました: {e}")
                            logger.error(traceback.format_exc())