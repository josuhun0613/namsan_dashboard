"""
청년회 사역 관리 대시보드
Google Spreadsheets를 백엔드로 사용하는 Streamlit 웹앱
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from typing import Tuple, Dict, List
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
    """
    Google Sheets API 연결

    secrets.toml 설정 방법:
    1. .streamlit/secrets.toml 파일 생성
    2. Google Cloud Console에서 서비스 계정 생성 및 JSON 키 다운로드
    3. 아래 형식으로 secrets.toml에 추가:

    [gcp_service_account]
    type = "service_account"
    project_id = "your-project-id"
    private_key_id = "key-id"
    private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
    client_email = "your-service-account@project.iam.gserviceaccount.com"
    client_id = "123456789"
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/..."

    4. Google Spreadsheet를 서비스 계정 이메일과 공유
    """
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
        st.info("secrets.toml 파일이 올바르게 설정되었는지 확인하세요.")
        return None

@st.cache_data(ttl=300)  # 5분 캐시
def load_data(_client) -> pd.DataFrame:
    """Google Sheets에서 Record_DB 시트 로드"""
    try:
        spreadsheet = _client.open('남산 대시보드')
        worksheet = spreadsheet.worksheet('Record_DB')

        # 모든 데이터 가져오기
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)

        # 날짜 컬럼 형식 변환
        df['날짜'] = pd.to_datetime(df['날짜'], format='%Y-%m-%d', errors='coerce')

        # 지표 컬럼을 숫자로 변환
        indicator_cols = ['전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조']
        for col in indicator_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        return df
    except Exception as e:
        st.error(f"데이터 로드 실패: {str(e)}")
        return pd.DataFrame()

def update_data_to_gsheet(client, df: pd.DataFrame, month_filter: str, region_filter: str, zone_filter: str):
    """수정된 데이터를 Google Sheets에 업데이트"""
    try:
        spreadsheet = client.open('남산 대시보드')
        worksheet = spreadsheet.worksheet('Record_DB')

        # 날짜를 문자열로 변환
        df_to_update = df.copy()
        df_to_update['날짜'] = df_to_update['날짜'].dt.strftime('%Y-%m-%d')

        # 전체 데이터 다시 로드
        all_data = worksheet.get_all_records()
        all_df = pd.DataFrame(all_data)

        # 수정된 행 업데이트
        for idx, row in df_to_update.iterrows():
            # 해당 행 찾기 (날짜, 이름, 지역, 구역으로 매칭)
            mask = (
                (all_df['날짜'] == row['날짜']) &
                (all_df['이름'] == row['이름']) &
                (all_df['지역'] == row['지역']) &
                (all_df['구역'] == row['구역'])
            )

            if mask.any():
                row_idx = all_df[mask].index[0]
                all_df.loc[row_idx] = row

        # 전체 데이터를 시트에 다시 쓰기
        worksheet.clear()
        worksheet.update([all_df.columns.values.tolist()] + all_df.values.tolist())

        return True
    except Exception as e:
        st.error(f"데이터 업데이트 실패: {str(e)}")
        return False

# ==================== 데이터 처리 함수 ====================
def get_available_months(df: pd.DataFrame) -> List[str]:
    """데이터에서 사용 가능한 월 목록 추출"""
    if df.empty or '날짜' not in df.columns:
        return []

    df['년월'] = df['날짜'].dt.strftime('%Y년 %m월')
    months = sorted(df['년월'].dropna().unique(), reverse=True)
    return months

def filter_data(df: pd.DataFrame, month: str, region: str = '전체', zone: str = '전체') -> pd.DataFrame:
    """월, 지역, 구역 필터 적용"""
    filtered = df.copy()

    # 월 필터
    if month and not df.empty:
        filtered['년월'] = filtered['날짜'].dt.strftime('%Y년 %m월')
        filtered = filtered[filtered['년월'] == month]

    # 지역 필터
    if region != '전체':
        filtered = filtered[filtered['지역'] == region]

    # 구역 필터
    if zone != '전체':
        filtered = filtered[filtered['구역'] == zone]

    return filtered

def calculate_active_members(df: pd.DataFrame) -> int:
    """재적 인원 수 계산 (상태가 '재적'인 사람만)"""
    if df.empty:
        return 0
    return len(df[df['상태'] == '재적']['이름'].unique())

def calculate_attendance_rate(df: pd.DataFrame) -> float:
    """출석률 계산 (재적 인원 기준)"""
    if df.empty:
        return 0.0

    active_df = df[df['상태'] == '재적']
    if len(active_df) == 0:
        return 0.0

    return (active_df['전체출결'].sum() / len(active_df)) * 100

def calculate_evangelism_rate(df: pd.DataFrame) -> float:
    """전도 이행률 계산 (재적 인원 기준)"""
    if df.empty:
        return 0.0

    active_df = df[df['상태'] == '재적']
    if len(active_df) == 0:
        return 0.0

    return (active_df['전도'].sum() / len(active_df)) * 100

def calculate_zone_scores(df: pd.DataFrame, weights: Dict[str, float]) -> pd.DataFrame:
    """구역별 종합 점수 계산"""
    if df.empty:
        return pd.DataFrame()

    # 재적 인원만 필터링
    active_df = df[df['상태'] == '재적'].copy()

    if active_df.empty:
        return pd.DataFrame()

    # 구역별 집계
    indicators = ['전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조']

    zone_stats = active_df.groupby('구역').agg({
        '이름': 'count',  # 재적 인원수
        **{ind: 'sum' for ind in indicators}
    }).rename(columns={'이름': '재적수'})

    # 비율 계산 (각 지표를 재적수로 나눔)
    for ind in indicators:
        zone_stats[f'{ind}_비율'] = (zone_stats[ind] / zone_stats['재적수'] * 100).round(1)

    # 종합 점수 계산 (가중치 적용)
    zone_stats['종합점수'] = 0.0
    for ind in indicators:
        weight = weights.get(ind, 0) / 100  # 퍼센트를 소수로 변환
        zone_stats['종합점수'] += zone_stats[f'{ind}_비율'] * weight

    zone_stats['종합점수'] = zone_stats['종합점수'].round(1)

    # 순위 추가
    zone_stats = zone_stats.sort_values('종합점수', ascending=False)
    zone_stats['순위'] = range(1, len(zone_stats) + 1)

    # 지역 정보 추가
    zone_region = active_df.groupby('구역')['지역'].first()
    zone_stats['지역'] = zone_region

    return zone_stats

def get_previous_month_data(df: pd.DataFrame, current_month: str) -> pd.DataFrame:
    """전월 데이터 가져오기"""
    if df.empty:
        return pd.DataFrame()

    try:
        current_date = datetime.strptime(current_month, '%Y년 %m월')
        prev_month = current_date.replace(day=1)

        if prev_month.month == 1:
            prev_month = prev_month.replace(year=prev_month.year - 1, month=12)
        else:
            prev_month = prev_month.replace(month=prev_month.month - 1)

        prev_month_str = prev_month.strftime('%Y년 %m월')
        return filter_data(df, prev_month_str)
    except:
        return pd.DataFrame()

# ==================== UI 렌더링 함수 ====================
def render_sidebar(df: pd.DataFrame) -> Tuple[str, str, str, Dict[str, float]]:
    """사이드바 렌더링"""
    st.sidebar.title("⛪ 청년회 대시보드")
    st.sidebar.markdown("---")

    # 기준 월 선택
    st.sidebar.subheader("📅 기준 설정")
    months = get_available_months(df)

    if not months:
        st.sidebar.warning("데이터가 없습니다.")
        selected_month = None
    else:
        selected_month = st.sidebar.selectbox(
            "기준 월",
            months,
            index=0
        )

    # 지역 필터
    if not df.empty and '지역' in df.columns:
        regions = ['전체'] + sorted(df['지역'].dropna().unique().tolist())
    else:
        regions = ['전체']

    selected_region = st.sidebar.selectbox("지역", regions)

    # 구역 필터 (선택된 지역에 따라 동적 변경)
    if selected_region != '전체' and not df.empty:
        zones = ['전체'] + sorted(
            df[df['지역'] == selected_region]['구역'].dropna().unique().tolist()
        )
    elif not df.empty and '구역' in df.columns:
        zones = ['전체'] + sorted(df['구역'].dropna().unique().tolist())
    else:
        zones = ['전체']

    selected_zone = st.sidebar.selectbox("구역", zones)

    # 가중치 설정
    st.sidebar.markdown("---")
    with st.sidebar.expander("⚖️ 종합지표 가중치 설정"):
        st.caption("각 지표의 중요도를 조절하세요 (합계 100%)")

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
            st.warning(f"⚠️ 가중치 합계: {total_weight:.1f}% (100%로 조정 필요)")
        else:
            st.success(f"✅ 가중치 합계: {total_weight:.1f}%")

    return selected_month, selected_region, selected_zone, weights

def render_kpi_cards(current_df: pd.DataFrame, previous_df: pd.DataFrame):
    """KPI 스코어카드 렌더링"""
    col1, col2, col3 = st.columns(3)

    # 현재 월 지표
    current_members = calculate_active_members(current_df)
    current_attendance = calculate_attendance_rate(current_df)
    current_evangelism = calculate_evangelism_rate(current_df)

    # 전월 지표
    prev_members = calculate_active_members(previous_df)
    prev_attendance = calculate_attendance_rate(previous_df)
    prev_evangelism = calculate_evangelism_rate(previous_df)

    # Delta 계산
    delta_members = current_members - prev_members
    delta_attendance = current_attendance - prev_attendance
    delta_evangelism = current_evangelism - prev_evangelism

    with col1:
        st.metric(
            label="👥 총 재적수",
            value=f"{current_members}명",
            delta=f"{delta_members:+d}명" if prev_members > 0 else None
        )

    with col2:
        st.metric(
            label="📊 평균 출석률",
            value=f"{current_attendance:.1f}%",
            delta=f"{delta_attendance:+.1f}%" if prev_attendance > 0 else None
        )

    with col3:
        st.metric(
            label="📢 전도 이행률",
            value=f"{current_evangelism:.1f}%",
            delta=f"{delta_evangelism:+.1f}%" if prev_evangelism > 0 else None
        )

def render_leaderboard(zone_scores: pd.DataFrame):
    """구역별 종합 랭킹 렌더링"""
    st.subheader("🏆 구역별 종합 랭킹")

    if zone_scores.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    # 표시할 컬럼 선택
    display_columns = ['순위', '지역', '재적수', '종합점수',
                       '전체출결_비율', '대면출결_비율', '마이심_비율',
                       '상시활동_비율', '전도_비율', '십일조_비율']

    display_df = zone_scores.reset_index()[display_columns].copy()

    # 컬럼명 변경
    display_df.columns = ['순위', '지역', '재적수', '종합점수',
                          '전체출결(%)', '대면출결(%)', '마이심(%)',
                          '상시활동(%)', '전도(%)', '십일조(%)']

    # 순위에 따라 메달 이모지 추가
    def add_medal(row):
        if row['순위'] == 1:
            return '🥇 1위'
        elif row['순위'] == 2:
            return '🥈 2위'
        elif row['순위'] == 3:
            return '🥉 3위'
        else:
            return f"{row['순위']}위"

    display_df['순위'] = display_df.apply(add_medal, axis=1)

    # 스타일 적용하여 표시
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=400
    )

def render_region_comparison(df: pd.DataFrame, weights: Dict[str, float]):
    """지역별 비교 Bar Chart"""
    st.subheader("📊 지역별 종합 점수 비교")

    if df.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    active_df = df[df['상태'] == '재적'].copy()

    if active_df.empty:
        st.info("재적 인원이 없습니다.")
        return

    # 지역별 점수 계산
    indicators = ['전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조']

    region_stats = active_df.groupby('지역').agg({
        '이름': 'count',
        **{ind: 'sum' for ind in indicators}
    }).rename(columns={'이름': '재적수'})

    # 비율 및 종합 점수 계산
    for ind in indicators:
        region_stats[f'{ind}_비율'] = (region_stats[ind] / region_stats['재적수'] * 100)

    region_stats['종합점수'] = 0.0
    for ind in indicators:
        weight = weights.get(ind, 0) / 100
        region_stats['종합점수'] += region_stats[f'{ind}_비율'] * weight

    # 차트 생성
    fig = px.bar(
        region_stats.reset_index(),
        x='지역',
        y='종합점수',
        text='종합점수',
        color='종합점수',
        color_continuous_scale='Blues',
        title='지역별 종합 점수'
    )

    fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
    fig.update_layout(showlegend=False, height=400)

    st.plotly_chart(fig, use_container_width=True)

def render_radar_chart(df: pd.DataFrame, region: str, weights: Dict[str, float]):
    """지표별 분석 Radar Chart"""
    st.subheader("🎯 지표별 강점 분석")

    if df.empty or region == '전체':
        st.info("특정 지역을 선택하면 해당 지역의 강점을 분석합니다.")
        return

    active_df = df[df['상태'] == '재적'].copy()

    if active_df.empty:
        st.info("재적 인원이 없습니다.")
        return

    # 선택된 지역 데이터
    region_df = active_df[active_df['지역'] == region]

    if region_df.empty:
        st.info(f"{region} 지역의 데이터가 없습니다.")
        return

    # 지표별 비율 계산
    indicators = ['전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조']
    categories = ['예배출석', '대면출석', '마이심', '상시활동', '전도', '십일조']

    values = []
    for ind in indicators:
        rate = (region_df[ind].sum() / len(region_df)) * 100
        values.append(rate)

    # Radar Chart 생성
    fig = go.Figure()

    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name=region,
        line_color='#1f77b4'
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )
        ),
        showlegend=True,
        title=f'{region} 지역 지표별 이행률',
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)

def render_missing_sheep(df: pd.DataFrame):
    """미참석자 리스트"""
    st.subheader("🐑 미참석자 명단")

    if df.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    # 재적 인원 중 전체출결이 0인 사람
    active_df = df[df['상태'] == '재적'].copy()
    missing = active_df[active_df['전체출결'] == 0].copy()

    if missing.empty:
        st.success("✅ 모든 재적 인원이 출석했습니다!")
        return

    # 최근 출석일 계산 (해당 인원의 전체 출석 기록에서)
    missing_list = []
    for _, row in missing.iterrows():
        person_history = df[
            (df['이름'] == row['이름']) &
            (df['전체출결'] == 1)
        ].sort_values('날짜', ascending=False)

        last_attendance = person_history['날짜'].iloc[0].strftime('%Y-%m-%d') if not person_history.empty else '기록 없음'

        missing_list.append({
            '이름': row['이름'],
            '지역': row['지역'],
            '구역': row['구역'],
            '직분': row['직분'],
            '최근 출석일': last_attendance
        })

    missing_df = pd.DataFrame(missing_list)

    st.dataframe(
        missing_df,
        use_container_width=True,
        hide_index=True
    )

    st.caption(f"총 {len(missing_df)}명의 미참석자가 있습니다.")

def render_data_editor(df: pd.DataFrame, client, month: str, region: str, zone: str):
    """데이터 수정 인터페이스"""
    st.subheader("✏️ 데이터 수정")

    if df.empty:
        st.info("수정할 데이터가 없습니다.")
        return

    st.caption("아래 표에서 직접 데이터를 수정할 수 있습니다. 수정 후 '저장' 버튼을 클릭하세요.")

    # 편집 가능한 컬럼 선택
    edit_columns = ['날짜', '이름', '지역', '구역', '직분', '상태',
                    '전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조']

    edit_df = df[edit_columns].copy()

    # 날짜를 문자열로 변환 (편집 용이성)
    if not edit_df.empty and '날짜' in edit_df.columns:
        edit_df['날짜'] = edit_df['날짜'].dt.strftime('%Y-%m-%d')

    # 데이터 에디터
    edited_df = st.data_editor(
        edit_df,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "날짜": st.column_config.TextColumn(
                "날짜"
            ),
            "상태": st.column_config.SelectboxColumn(
                "상태",
                options=["재적", "제외"],
                required=True
            ),
            "전체출결": st.column_config.NumberColumn(
                "전체출결",
                min_value=0,
                max_value=1,
                step=1
            ),
            "대면출결": st.column_config.NumberColumn(
                "대면출결",
                min_value=0,
                max_value=1,
                step=1
            ),
            "마이심": st.column_config.NumberColumn(
                "마이심",
                min_value=0,
                max_value=1,
                step=1
            ),
            "상시활동": st.column_config.NumberColumn(
                "상시활동",
                min_value=0,
                max_value=1,
                step=1
            ),
            "전도": st.column_config.NumberColumn(
                "전도",
                min_value=0,
                max_value=1,
                step=1
            ),
            "십일조": st.column_config.NumberColumn(
                "십일조",
                min_value=0,
                max_value=1,
                step=1
            )
        }
    )

    # 저장 버튼
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("💾 저장", type="primary", use_container_width=True):
            with st.spinner("저장 중..."):
                # 날짜를 datetime으로 다시 변환
                edited_df['날짜'] = pd.to_datetime(edited_df['날짜'])

                success = update_data_to_gsheet(client, edited_df, month, region, zone)

                if success:
                    st.success("✅ 데이터가 성공적으로 저장되었습니다!")
                    st.cache_data.clear()  # 캐시 초기화
                    st.rerun()  # 페이지 새로고침
                else:
                    st.error("❌ 데이터 저장에 실패했습니다.")

    with col2:
        st.caption("⚠️ 저장하기 전에 수정한 내용을 다시 확인하세요.")

# ==================== 메인 앱 ====================
def main():
    # Google Sheets 연결
    client = connect_to_gsheet()

    if client is None:
        st.error("Google Sheets에 연결할 수 없습니다. secrets.toml 설정을 확인하세요.")
        return

    # 데이터 로드
    df = load_data(client)

    if df.empty:
        st.warning("데이터가 없습니다. Google Spreadsheet를 확인하세요.")
        return

    # 사이드바 렌더링
    selected_month, selected_region, selected_zone, weights = render_sidebar(df)

    # 데이터 필터링
    filtered_df = filter_data(df, selected_month, selected_region, selected_zone)
    previous_df = get_previous_month_data(df, selected_month) if selected_month else pd.DataFrame()

    # 메인 타이틀
    st.title("⛪ 청년회 사역 관리 대시보드")
    st.markdown(f"**선택된 기간:** {selected_month or '전체'} | **지역:** {selected_region} | **구역:** {selected_zone}")
    st.markdown("---")

    # 탭 생성
    tab1, tab2 = st.tabs(["📊 대시보드", "👥 관리 및 심방"])

    # 탭 1: 대시보드
    with tab1:
        # KPI 카드
        render_kpi_cards(filtered_df, previous_df)

        st.markdown("---")

        # 종합 랭킹
        zone_scores = calculate_zone_scores(filtered_df, weights)
        render_leaderboard(zone_scores)

        st.markdown("---")

        # 시각화
        col1, col2 = st.columns(2)

        with col1:
            render_region_comparison(filtered_df, weights)

        with col2:
            render_radar_chart(filtered_df, selected_region, weights)

    # 탭 2: 관리 및 심방
    with tab2:
        # 미참석자 리스트
        render_missing_sheep(filtered_df)

        st.markdown("---")

        # 데이터 수정
        render_data_editor(filtered_df, client, selected_month, selected_region, selected_zone)

if __name__ == "__main__":
    main()
