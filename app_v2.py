"""
청년회 사역 관리 대시보드 V2
구역별 월간 집계표 중심의 대시보드
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==================== 페이지 설정 ====================
st.set_page_config(
    page_title="청년회 사역 관리 대시보드",
    page_icon="⛪",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== Google Sheets 연결 ====================
@st.cache_resource
def connect_to_gsheet():
    """Google Sheets API 연결"""
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]

        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            st.secrets["gcp_service_account"],
            scope
        )

        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        st.error(f"Google Sheets 연결 실패: {str(e)}")
        return None

@st.cache_data(ttl=300)
def load_all_zone_data(_client) -> Dict[str, pd.DataFrame]:
    """
    모든 구역 시트에서 데이터 로드
    각 구역 시트 형식: 날짜(행) x 인원(열)
    """
    try:
        spreadsheet = _client.open('남산 대시보드')

        # 모든 시트 가져오기
        worksheets = spreadsheet.worksheets()

        zone_data = {}

        for worksheet in worksheets:
            sheet_name = worksheet.title

            # Record_DB 시트는 건너뛰기
            if sheet_name == 'Record_DB':
                continue

            # 구역 시트만 처리 (예: 1구역, 2구역, ...)
            if '구역' in sheet_name:
                data = worksheet.get_all_records()
                if data:
                    df = pd.DataFrame(data)
                    zone_data[sheet_name] = df

        return zone_data
    except Exception as e:
        st.error(f"데이터 로드 실패: {str(e)}")
        return {}

def calculate_zone_summary(zone_df: pd.DataFrame, month: str, zone_name: str, region: str) -> Dict:
    """
    구역별 월간 요약 통계 계산

    데이터 형식 예시:
    날짜       | 이름 | 직분 | 상태 | 전체출결 | 대면출결 | 마이심 | 상시활동 | 전도 | 십일조
    2024-09-01 | 홍길동 | 청년 | 재적 | 1 | 1 | 1 | 0 | 1 | 1
    """
    if zone_df.empty:
        return None

    # 해당 월 데이터 필터링
    zone_df['날짜'] = pd.to_datetime(zone_df['날짜'], format='%Y-%m-%d', errors='coerce')
    year, month_num = month.split('년 ')
    month_num = month_num.replace('월', '').strip()

    month_df = zone_df[
        (zone_df['날짜'].dt.year == int(year)) &
        (zone_df['날짜'].dt.month == int(month_num))
    ]

    if month_df.empty:
        return None

    # 재적 인원만 필터링
    active_df = month_df[month_df['상태'] == '재적']

    if active_df.empty:
        return None

    # 구역장 찾기 (직분이 '리더' 또는 '구역장'인 사람)
    leader = active_df[active_df['직분'].isin(['리더', '구역장', '간사'])]['이름'].values
    leader_name = leader[0] if len(leader) > 0 else '-'

    # 통계 계산
    total_members = len(active_df['이름'].unique())

    indicators = {
        '전체출결': active_df['전체출결'].sum() / total_members * 100,
        '대면출결': active_df['대면출결'].sum() / total_members * 100,
        '마이심': active_df['마이심'].sum() / total_members * 100,
        '상시활동': active_df['상시활동'].sum() / total_members * 100,
        '전도': active_df['전도'].sum() / total_members * 100,
        '십일조': active_df['십일조'].sum() / total_members * 100,
    }

    # 한자율 계산 (전도한 사람 비율)
    han_ja_rate = indicators['전도']

    return {
        '지역': region,
        '구역': zone_name,
        '구역장': leader_name,
        '재적': total_members,
        '전체출결': round(indicators['전체출결']),
        '대면출결': round(indicators['대면출결']),
        '마이심': round(indicators['마이심']),
        '상시활동': round(indicators['상시활동']),
        '전도': round(indicators['전도']),
        '한자율': round(han_ja_rate),
        '십일조': round(indicators['십일조']),
    }

def calculate_comprehensive_score(row: pd.Series, weights: Dict[str, float]) -> float:
    """종합지표 계산"""
    indicators = ['전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조']

    score = 0.0
    for ind in indicators:
        if ind in row and ind in weights:
            score += row[ind] * (weights[ind] / 100)

    return round(score, 1)

def get_available_months(zone_data: Dict[str, pd.DataFrame]) -> list:
    """모든 구역 데이터에서 사용 가능한 월 추출"""
    all_dates = []

    for df in zone_data.values():
        if not df.empty and '날짜' in df.columns:
            df_copy = df.copy()
            df_copy['날짜'] = pd.to_datetime(df_copy['날짜'], errors='coerce')
            all_dates.extend(df_copy['날짜'].dropna().tolist())

    if not all_dates:
        return []

    dates_df = pd.DataFrame({'날짜': all_dates})
    dates_df['년월'] = dates_df['날짜'].dt.strftime('%Y년 %m월')

    return sorted(dates_df['년월'].unique(), reverse=True)

# ==================== UI 렌더링 ====================
def render_sidebar() -> tuple:
    """사이드바 렌더링"""
    st.sidebar.title("⛪ 청년회 대시보드")
    st.sidebar.markdown("---")

    # 기준 월 선택
    st.sidebar.subheader("📅 기준 월")

    # 가중치 설정
    st.sidebar.markdown("---")
    with st.sidebar.expander("⚖️ 종합지표 가중치 설정"):
        st.caption("각 지표의 중요도를 조절하세요")

        indicators = ['전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조']
        default_weight = 100 / len(indicators)

        weights = {}
        for ind in indicators:
            weights[ind] = st.slider(
                ind,
                min_value=0.0,
                max_value=100.0,
                value=default_weight,
                step=5.0,
                key=f"weight_{ind}"
            )

        total_weight = sum(weights.values())
        if abs(total_weight - 100) > 0.1:
            st.warning(f"⚠️ 가중치 합계: {total_weight:.1f}%")
        else:
            st.success(f"✅ 가중치 합계: {total_weight:.1f}%")

    return weights

def render_monthly_summary_table(summary_df: pd.DataFrame, month: str):
    """월간 구역별 집계표 렌더링 (이미지 스타일)"""

    if summary_df.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    st.subheader(f"📊 {month} 구역별 성적표")

    # 지역별로 그룹화
    regions = summary_df['지역'].unique()

    for region in sorted(regions):
        region_df = summary_df[summary_df['지역'] == region].copy()

        if region_df.empty:
            continue

        st.markdown(f"### {region} 지역")

        # 표시할 컬럼 순서
        display_columns = ['구역', '구역장', '재적', '전체출결', '대면출결',
                          '마이심', '상시활동', '전도', '한자율', '십일조', '종합지표', '순위']

        display_df = region_df[display_columns].copy()

        # 퍼센트 기호 추가
        percent_columns = ['전체출결', '대면출결', '마이심', '상시활동', '전도', '한자율', '십일조', '종합지표']
        for col in percent_columns:
            display_df[col] = display_df[col].apply(lambda x: f"{x}%")

        # HTML 테이블로 변환하여 스타일 적용
        def highlight_rank(row):
            """순위에 따라 행 색상 지정"""
            rank = int(row['순위'])
            if rank == 1:
                return ['background-color: #FFEB3B'] * len(row)  # 노란색
            elif rank == 2:
                return ['background-color: #B0E0E6'] * len(row)  # 하늘색
            elif rank == 3:
                return ['background-color: #98FB98'] * len(row)  # 연두색
            else:
                return [''] * len(row)

        styled_df = display_df.style.apply(highlight_rank, axis=1)

        st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True
        )

        st.markdown("---")

    # 전체 지역 통합 통계
    st.markdown("### 📈 전체 지역 통합")

    # 지역별 평균 계산
    region_summary = summary_df.groupby('지역').agg({
        '재적': 'sum',
        '전체출결': 'mean',
        '대면출결': 'mean',
        '마이심': 'mean',
        '상시활동': 'mean',
        '전도': 'mean',
        '십일조': 'mean',
        '종합지표': 'mean'
    }).reset_index()

    region_summary = region_summary.round(1)

    # 퍼센트 추가
    for col in ['전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조', '종합지표']:
        region_summary[col] = region_summary[col].apply(lambda x: f"{x}%")

    st.dataframe(
        region_summary,
        use_container_width=True,
        hide_index=True
    )

# ==================== 메인 앱 ====================
def main():
    # Google Sheets 연결
    client = connect_to_gsheet()

    if client is None:
        st.error("Google Sheets에 연결할 수 없습니다.")
        return

    # 모든 구역 데이터 로드
    zone_data = load_all_zone_data(client)

    if not zone_data:
        st.warning("구역 데이터가 없습니다. Google Spreadsheet에 구역 시트를 생성하세요.")
        st.info("예: 1구역, 2구역, 3구역 등의 이름으로 시트를 생성하고 데이터를 입력하세요.")
        return

    # 사이드바
    weights = render_sidebar()

    # 사용 가능한 월 목록
    available_months = get_available_months(zone_data)

    if not available_months:
        st.warning("데이터에서 날짜 정보를 찾을 수 없습니다.")
        return

    # 사이드바에 월 선택 추가
    selected_month = st.sidebar.selectbox(
        "기준 월 선택",
        available_months,
        index=0
    )

    # 메인 타이틀
    st.title(f"⛪ {selected_month} 청년회 성적표")
    st.markdown("---")

    # 구역별 요약 데이터 생성
    summary_list = []

    # 지역 정보 매핑 (구역 번호로 지역 판단)
    zone_region_map = {
        '1구역': '도원', '2구역': '도원', '3구역': '도원',
        '4구역': '송파', '5구역': '송파', '6구역': '송파',
        '7구역': '강동', '8구역': '강동', '9구역': '강동',
    }

    for zone_name, zone_df in zone_data.items():
        region = zone_region_map.get(zone_name, '기타')

        summary = calculate_zone_summary(zone_df, selected_month, zone_name, region)

        if summary:
            summary_list.append(summary)

    if not summary_list:
        st.warning(f"{selected_month}에 대한 데이터가 없습니다.")
        return

    # DataFrame 생성
    summary_df = pd.DataFrame(summary_list)

    # 종합지표 계산
    summary_df['종합지표'] = summary_df.apply(
        lambda row: calculate_comprehensive_score(row, weights),
        axis=1
    )

    # 순위 계산 (종합지표 기준)
    summary_df = summary_df.sort_values('종합지표', ascending=False)
    summary_df['순위'] = range(1, len(summary_df) + 1)

    # 월간 집계표 렌더링
    render_monthly_summary_table(summary_df, selected_month)

if __name__ == "__main__":
    main()
